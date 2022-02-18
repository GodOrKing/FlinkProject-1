package cn.itcast.streaming.function.asyncio;

import cn.itcast.entity.ItcastDataPartObj;
import cn.itcast.entity.OnlineDataObj;
import cn.itcast.entity.VehicleLocationModel;
import cn.itcast.utils.GaoDeMapUtils;
import cn.itcast.utils.GeoHashUtil;
import cn.itcast.utils.RedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 实现异步访问高德api接口，传递经纬度信息返回地理位置信息
 */
public class AsyncHttpQueryFunction extends RichAsyncFunction<ItcastDataPartObj, ItcastDataPartObj> {

    //定义httpclient对象
    CloseableHttpAsyncClient httpAsyncClient = null;

    /**
     * 初始化资源
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //创建一个异步的httpClient的连接池
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)//设置httpClient连接池超时时间
                .build();

        //初始化异步的HttpClient
        httpAsyncClient = HttpAsyncClients.custom()
                .setMaxConnTotal(20) //连接池最大的连接数据
                .setDefaultRequestConfig(requestConfig)
                .build();

        httpAsyncClient.start();
    }

    /**
     * 关闭连接，释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        httpAsyncClient.close();
    }

    /**
     * 重写超时方法
     * @param input
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void timeout(ItcastDataPartObj input, ResultFuture<ItcastDataPartObj> resultFuture) throws Exception {
        System.out.println("访问高德api访问接口的时候超时了...");
    }

    /**
     * 异步处理函数的主执行方法
     * @param itcastDataPartObj
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(ItcastDataPartObj itcastDataPartObj, ResultFuture<ItcastDataPartObj> resultFuture) throws Exception {
        //TODO 1：获取到经度和维度信息
        double longitude = itcastDataPartObj.getLng();
        double latitude = itcastDataPartObj.getLat();

        try {
            //todo 2：创建httpGet请求地址
            String gaoDeAddressUrl = GaoDeMapUtils.getUrlByLonLat(longitude, latitude);

            //todo 3：创建http请求对象
            HttpPost httpPost = new HttpPost(gaoDeAddressUrl);

            //todo 4：递交httpclient请求，获取异步请求的futrue对象
            Future<HttpResponse> future = httpAsyncClient.execute(httpPost, null);//callback是回调函数（也可以通过回调函数实现结果的获取）

            //todo 5：从成功的future对象中获取数据，返回javaBean对象
            CompletableFuture<ItcastDataPartObj> onlineDataObjCompletableFuture = CompletableFuture.supplyAsync(new Supplier<ItcastDataPartObj>() {
                //获取到返回值
                @Override
                public ItcastDataPartObj get() {
                    try {
                        //使用future获取到返回值
                        HttpResponse httpResponse = future.get();
                        //省份
                        String province = null;
                        //城市
                        String city = null;
                        //国家
                        String country = null;
                        //区县
                        String district = null;
                        //详细地址
                        String address = null;

                        //判断返回值的状态是否正常
                        if(httpResponse.getStatusLine().getStatusCode() == 200){
                            //拿到响应的实例对象
                            HttpEntity httpEntity = httpResponse.getEntity();
                            //将实例对象转换成string字符串
                            String result = EntityUtils.toString(httpEntity);

                            //将返回的json字符串反序列化成jsonObject
                            JSONObject jsonObject = JSON.parseObject(result);

                            JSONObject regeocodeObject = jsonObject.getJSONObject("regeocode");
                            if(regeocodeObject!= null && !regeocodeObject.isEmpty()){
                                JSONObject addressComponent = regeocodeObject.getJSONObject("addressComponent");
                                country = addressComponent.getString("country");
                                province = addressComponent.getString("province");
                                city = addressComponent.getString("city");
                                district = addressComponent.getString("district");
                                address = regeocodeObject.getString("formatted_address");

                                VehicleLocationModel tmpVehicleLocationModel = new VehicleLocationModel(
                                    province, city, country, district, address, latitude, longitude
                                );
                                String geoHash = GeoHashUtil.encode(latitude, longitude);
                                RedisUtil.set(geoHash.getBytes(), JSON.toJSONString(tmpVehicleLocationModel).getBytes());
                            }
                        }
                        itcastDataPartObj.setProvince(province);
                        itcastDataPartObj.setCity(city);
                        itcastDataPartObj.setCountry(country);
                        itcastDataPartObj.setDistrict(district);
                        itcastDataPartObj.setAddress(address);

                        //返回javaBean对象
                        return itcastDataPartObj;
                    } catch (Exception e) {
                        return null;
                    }
                }
            });

            //todo 6：将取出来的存入到ResultFuture，返回给方法
            onlineDataObjCompletableFuture.thenAccept(new Consumer<ItcastDataPartObj>() {
                @Override
                public void accept(ItcastDataPartObj itcastDataPartObj) {
                    //complete()里面需要的是collection集合，但是一次执行只能返回一个结果
                    //所以集合使用singleton单例模式，集合中只放一个对象
                    resultFuture.complete(Collections.singleton(itcastDataPartObj));
                }
            });
        } catch (Exception exception) {
            resultFuture.complete(Collections.singleton(null));
        }
    }
}
