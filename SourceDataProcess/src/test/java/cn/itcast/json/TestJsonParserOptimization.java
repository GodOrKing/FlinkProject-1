package cn.itcast.json;

import cn.itcast.json.bean.CarJsonPlusBean;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.DocFlavor;
import javax.swing.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * json字符串解析优化后的方案
 * tsp上报的json格式与其他的终端上报的json格式不同（差异），因此按照固定的参数进行解析数据一定会存在解析异常的问题，因此需要对日志解析进行优化
 * {"batteryAlarm": 0,"carMode": 1,"minVoltageBattery": 3.89,"chargeStatus": 1,"vin": "LS5A3CJC0JF890971","nevChargeSystemTemperatureDtoList":
 *  [{"probeTemperatures": [25, 23, 24, 21, 24, 21, 23, 21, 23, 21, 24, 21, 24, 21, 25, 21],"chargeTemperatureProbeNum": 16,"childSystemNum": 1}]}
 */
public class TestJsonParserOptimization {
    private static Logger logger = LoggerFactory.getLogger(TestJsonParserOptimization.class);

    public static void main(String[] args) {
        /**
         * 实现步骤：
         * 1）定义json字符串
         * 2）定义map对象，将json的所有属性和数据值解析后追加到map对象中
         * 3）根据key获取map对象的value，如果存在key，则获取到value，如果不存在key则返回null，或者初始化一个默认值
         * 4）打印测试
         */

        //TODO 1）定义json字符串
        String jsonStr = "{\"batteryAlarm\": 0,\"carMode\": 1,\"minVoltageBattery\": 3.89,\"chargeStatus\": 1,\"vin\": \"LS5A3CJC0JF890971\",\"nevChargeSystemTemperatureDtoList\": [{\"probeTemperatures\": [25, 23, 24, 21, 24, 21, 23, 21, 23, 21, 24, 21, 24, 21, 25, 21],\"chargeTemperatureProbeNum\": 16,\"childSystemNum\": 1}]}";

        //TODO 2）定义map对象，将json的所有属性和数据值解析后追加到map对象中
        HashMap<String, Object> resultMap = jsonStrToMap(jsonStr);

        //TODO 3）根据key获取map对象的value，如果存在key，则获取到value，如果不存在key则返回null，或者初始化一个默认值
        int batteryAlarm = Integer.parseInt(resultMap.getOrDefault("batteryAlarm", -1).toString());
        int carMode = Integer.parseInt(resultMap.getOrDefault("carMode", -1).toString());
        double minVoltageBattery = Double.parseDouble(resultMap.getOrDefault("minVoltageBattery", -1).toString());
        int chargeStatus = Integer.parseInt(resultMap.getOrDefault("chargeStatus", -1).toString());
        String vin = resultMap.getOrDefault("vin", -1).toString();
        String nevChargeSystemTemperatureDtoList = resultMap.getOrDefault("nevChargeSystemTemperatureDtoList", "").toString();
        List<HashMap<String, Object>> jsonStrToList = jsonStrToList(nevChargeSystemTemperatureDtoList);

        int chargeTemperatureProbeNum = 0;
        int childSystemNum = 0;
        List<Integer> probeTemperaturesList = new ArrayList<>();
        //集合中存在数据
        if(jsonStrToList.size()>0){
            HashMap<String, Object> hashMap = jsonStrToList.get(0);
            String probeTemperatures = hashMap.getOrDefault("probeTemperatures", "").toString();

            JSONArray probeTemperaturesArray = new JSONArray(probeTemperatures);
            probeTemperaturesArray.forEach(obj -> {
                int value = Integer.parseInt(obj.toString());
                probeTemperaturesList.add(value);
            });
            chargeTemperatureProbeNum = Integer.parseInt(hashMap.getOrDefault("chargeTemperatureProbeNum", -1).toString());
            childSystemNum = Integer.parseInt(hashMap.getOrDefault("childSystemNum", -1).toString());
        }

        CarJsonPlusBean carJsonPlusBean = new CarJsonPlusBean(batteryAlarm, carMode, minVoltageBattery, chargeStatus, vin,
                probeTemperaturesList, chargeTemperatureProbeNum, childSystemNum);

        //TODO 4）打印测试
        logger.warn(carJsonPlusBean.toString());
    }

    /**
     * 将json字符串转换成list对象
     * @param jsonStr
     * @return
     */
    private static List<HashMap<String, Object>> jsonStrToList(String jsonStr){
        //定义jsonArray对象
        JSONArray jsonArray = new JSONArray(jsonStr);
        //创建需要返回的集合对象
        List<HashMap<String, Object>> resultList = new ArrayList<>();
        for (int i = 0; i <jsonArray.length() ; i++) {
            HashMap<String, Object> jsonMap = jsonStrToMap(jsonArray.get(i).toString());
            resultList.add(jsonMap);
        }

        //返回集合对象
        return resultList;
    }

    /**
     * 将json字符串转换成map对象
     * @param jsonStr
     * @return
     */
    private static HashMap<String, Object> jsonStrToMap(String jsonStr) {
        //定义jsonObject对象
        JSONObject jsonObject = new JSONObject(jsonStr);
        //创建需要返回的hashMap对象
        HashMap<String, Object> resultMap = new HashMap<String, Object>();

        //获取jsonObject对象的所有key集合
        Set<String> keySet = jsonObject.keySet();
        //遍历key的集合
        for (String key : keySet){
            Object value = jsonObject.get(key);
            resultMap.put(key, value);
        }
        //返回map对象
        return resultMap;
    }
}
