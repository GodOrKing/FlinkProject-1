package cn.itcast.json;

import cn.itcast.json.bean.CarJsonPlusBean;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 复杂json格式的解析
 * {"batteryAlarm": 0,"carMode": 1,"minVoltageBattery": 3.89,"chargeStatus": 1,"vin": "LS5A3CJC0JF890971","nevChargeSystemTemperatureDtoList":
 *  [{"probeTemperatures": [25, 23, 24, 21, 24, 21, 23, 21, 23, 21, 24, 21, 24, 21, 25, 21],"chargeTemperatureProbeNum": 16,"childSystemNum": 1}]}
 */
public class TestJsonParserPlus {
    public static void main(String[] args) {
        /**
         * 实现步骤：
         * 1）定义json字符串
         * 2）创建javaBean对象及定义属性
         * 3）使用jsonObject进行解析json字符串
         */

        //TODO 1）定义json字符串
        String jsonStr = "{\"batteryAlarm\": 0,\"carMode\": 1,\"minVoltageBattery\": 3.89,\"chargeStatus\": 1,\"vin\": \"LS5A3CJC0JF890971\",\"nevChargeSystemTemperatureDtoList\": [{\"probeTemperatures\": [25, 23, 24, 21, 24, 21, 23, 21, 23, 21, 24, 21, 24, 21, 25, 21],\"chargeTemperatureProbeNum\": 16,\"childSystemNum\": 1}]}";

        //TODO 2）创建javaBean对象及定义属性
        JSONObject jsonObject = new JSONObject(jsonStr);
        int batteryAlarm = jsonObject.getInt("batteryAlarm");
        int carMode = jsonObject.getInt("carMode");
        double minVoltageBattery = jsonObject.getDouble("minVoltageBattery");
        int chargeStatus = jsonObject.getInt("chargeStatus");
        String vin = jsonObject.getString("vin");
        //获取数组对象
        //2.1：第一次解析参数
        JSONArray nevChargeSystemTemperatureDtoList = jsonObject.getJSONArray("nevChargeSystemTemperatureDtoList");
        //2.2：默认获取数组中的第一条数据
        String nevChargeSystemTemperatureDtoInfoStr = nevChargeSystemTemperatureDtoList.get(0).toString();
        //2.3：第二次解析参数
        JSONObject jsonObject2 = new JSONObject(nevChargeSystemTemperatureDtoInfoStr);
        JSONArray probeTemperatures = jsonObject2.getJSONArray("probeTemperatures");
        //2.4：循环获取probeTemperatures对象的每一个元素
        List<Integer> probeTemperaturesResult = new ArrayList<Integer>();
        for (int i = 0; i < probeTemperatures.length(); i++) {
            int probeTemperaturesInt = probeTemperatures.getInt(i);
            probeTemperaturesResult.add(probeTemperaturesInt);
        }
        int chargeTemperatureProbeNum = jsonObject2.getInt("chargeTemperatureProbeNum");
        int childSystemNum = jsonObject2.getInt("childSystemNum");

        //TODO 3）使用jsonObject进行解析json字符串
        CarJsonPlusBean carJsonPlusBean = new CarJsonPlusBean(batteryAlarm, carMode, minVoltageBattery, chargeStatus, vin, probeTemperaturesResult,
                chargeTemperatureProbeNum, childSystemNum);

        System.out.println(carJsonPlusBean);
    }
}
