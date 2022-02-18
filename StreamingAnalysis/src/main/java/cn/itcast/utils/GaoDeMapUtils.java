package cn.itcast.utils;

import com.google.inject.internal.cglib.proxy.$LazyLoader;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 定义高德api访问的工具类
 */
public class GaoDeMapUtils {

    //指定高德地图请求的密钥
    private static final String KEY = ConfigLoader.getProperty("gaode.key");
    //指定返回值类型
    private static  final  String OUTPUT = "json";
    //请求的地址
    private static final String GET_ADDRESS_URL = ConfigLoader.getProperty("gaode.address.url");

    /**
     * 传递经纬度返回逆地理位置查询的请求地址
     * @param longitude
     * @param latitude
     * @return
     */
    public static String getUrlByLonLat(double longitude, double latitude) {
        //拼接经纬度的字符串参数
        String location = longitude + ","+latitude;
        //定义参数的集合对象
        Map<String, String> params = new HashMap<>();
        params.put("location", location);

        //根据请求base地址和参数集合列表拼接出来请求的完整地址
        String url = joinUrl(params, GET_ADDRESS_URL);
        return url;
    }

    /**
     * 拼接请求的参数和请求地址
     * @param params
     */
    private static String joinUrl(Map<String, String> params, String url) {
        StringBuilder baseUrl = new StringBuilder();
        baseUrl.append(url);

        try {
            //指定参数的索引
            int index = 0;
            Set<Map.Entry<String, String>> entries = params.entrySet();
            for (Map.Entry<String, String> param : entries) {
                if(index == 0){
                    baseUrl.append("?");
                }else{
                    baseUrl.append("&");
                }
                //拼接所有的参数
                baseUrl.append(param.getKey()).append("=").append(URLEncoder.encode(param.getValue(), "utf-8"));
            }
            baseUrl.append("&output=").append(OUTPUT).append("&key=").append(KEY);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return baseUrl.toString();
    }

}
