package cn.itcast.json;

import cn.itcast.json.bean.CarJsonBean;
import org.json.JSONObject;

/**
 * 需求：解析json字符串
 * json字符串的格式：{"batteryAlarm": 0, "carMode": 1,"minVoltageBattery": 3.89, "chargeStatus": 1,"vin":" LS5A3CJC0JF890971"}
 */
public class TestJsonParser {
    public static void main(String[] args) {
        /**
         * 解析步骤：
         * 1）定义json字符串
         * 2）定义json对应的JavaBean数据的对象及属性
         * 3）使用JsonObject解析json字符串
         */
        //TODO 1）定义json字符串
        String jsonStr = "{\"batteryAlarm\": 0, \"carMode\": 1,\"minVoltageBattery\": 3.89, \"chargeStatus\": 1,\"vin\":\" LS5A3CJC0JF890971\"}";

        //TODO 2）定义json对应的JavaBean数据的对象及属性
        JSONObject jsonObject = new JSONObject(jsonStr);
        int batteryAlarm = jsonObject.getInt("batteryAlarm");
        int carMode = jsonObject.getInt("carMode");
        double minVoltageBattery= jsonObject.getDouble("minVoltageBattery");
        int chargeStatus = jsonObject.getInt("chargeStatus");
        String vin = jsonObject.getString("vin");

        //TODO 3）使用JsonObject解析json字符串
        CarJsonBean carJsonBean = new CarJsonBean(batteryAlarm, carMode, minVoltageBattery, chargeStatus, vin);
        System.out.println(carJsonBean);
    }
}
