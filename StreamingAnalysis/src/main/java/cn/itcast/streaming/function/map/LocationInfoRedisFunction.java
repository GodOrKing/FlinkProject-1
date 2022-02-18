package cn.itcast.streaming.function.map;

import cn.itcast.entity.ItcastDataPartObj;
import cn.itcast.entity.OnlineDataObj;
import cn.itcast.entity.VehicleLocationModel;
import cn.itcast.utils.GeoHashUtil;
import cn.itcast.utils.RedisUtil;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 自定义map函数
 * 实现合并流数据于redis存储的地理位置数据拉宽操作
 */
public class LocationInfoRedisFunction extends RichMapFunction<ItcastDataPartObj, ItcastDataPartObj> {
    /**
     * 对合并流数据拉宽操作
     * @param itcastDataPartObj
     * @return
     * @throws Exception
     */
    @Override
    public ItcastDataPartObj map(ItcastDataPartObj itcastDataPartObj) throws Exception {
        //获取到原始数据的经度和维度
        String geoHash = GeoHashUtil.encode(itcastDataPartObj.getLat(), itcastDataPartObj.getLng());
        //根据geohash获取到redis的value值（geohash在redis中是作为主键存在）
        byte[] locationInfo = RedisUtil.get(geoHash.getBytes());
        if(locationInfo!=null){
            //redis中已经存储了该经纬度所对应的地理位置信息
            //将redis的value字段转换成javaBean对象
            VehicleLocationModel vehicleLocationModel = JSON.parseObject(new String(locationInfo), VehicleLocationModel.class);
            System.out.println("在redis获取的数据："+vehicleLocationModel);
            if(vehicleLocationModel!=null){
                itcastDataPartObj.setProvince(vehicleLocationModel.getProvince());
                itcastDataPartObj.setCity(vehicleLocationModel.getCity());
                itcastDataPartObj.setCountry(vehicleLocationModel.getCountry());
                itcastDataPartObj.setDistrict(vehicleLocationModel.getDistrict());
                itcastDataPartObj.setAddress(vehicleLocationModel.getAddress());
            }else{
                itcastDataPartObj.setProvince(null);
                itcastDataPartObj.setCity(null);
                itcastDataPartObj.setCountry(null);
                itcastDataPartObj.setDistrict(null);
                itcastDataPartObj.setAddress(null);
                System.out.println("在redis中没有获取到经纬度所对应的地理位置信息...");
            }
        }else{
            itcastDataPartObj.setProvince(null);
            itcastDataPartObj.setCity(null);
            itcastDataPartObj.setCountry(null);
            itcastDataPartObj.setDistrict(null);
            itcastDataPartObj.setAddress(null);
            System.out.println("在redis中没有获取到经纬度所对应的地理位置信息...");
        }
        //返回数据
        return itcastDataPartObj;
    }
}
