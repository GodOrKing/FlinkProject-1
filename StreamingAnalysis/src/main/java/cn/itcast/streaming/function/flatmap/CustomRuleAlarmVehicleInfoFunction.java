package cn.itcast.streaming.function.flatmap;

import cn.itcast.entity.CustomRuleAlarmResultModel;
import cn.itcast.entity.ItcastDataPartObj;
import cn.itcast.entity.VehicleInfoModel;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 自定义告警规则窗口流数据与车辆基础信息广播流数据进行拉宽操作
 */
public class CustomRuleAlarmVehicleInfoFunction  implements CoFlatMapFunction<
        ArrayList<ItcastDataPartObj>,
        HashMap<String, VehicleInfoModel>,
        ArrayList<Tuple2<ItcastDataPartObj, VehicleInfoModel>>> {
    //定义全局的车辆基础信息广播流对象
    HashMap<String, VehicleInfoModel> vehicleInfoModelHashMap = new HashMap<>();

    /**
     * 对自定告警规则窗口流数据应用的flatmap方法
     * @param value
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap1(ArrayList<ItcastDataPartObj> value, Collector<ArrayList<Tuple2<ItcastDataPartObj, VehicleInfoModel>>> collector) throws Exception {
        //定义需要返回的列表对象
        ArrayList<Tuple2<ItcastDataPartObj, VehicleInfoModel>> resultList = new ArrayList<>();

        //将窗口数据集合对象中的每条数据进行遍历操作
        value.forEach(itcastDataPartObj -> {
            Tuple2<ItcastDataPartObj, VehicleInfoModel> tuple2 = new Tuple2<>();
            VehicleInfoModel vehicleModel = vehicleInfoModelHashMap.getOrDefault(itcastDataPartObj.getVin(), new VehicleInfoModel());
            tuple2.f0 = itcastDataPartObj;
            tuple2.f1 = vehicleModel;
            //将元祖对象追加到集合列表中
            resultList.add(tuple2);
        });

        //将resultlist集合对象返回
        collector.collect(resultList);
    }

    /**
     * 对车辆基础信息广播流应用的flatmap方法
     * @param stringVehicleInfoModelHashMap
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap2(HashMap<String, VehicleInfoModel> stringVehicleInfoModelHashMap, Collector<ArrayList<Tuple2<ItcastDataPartObj, VehicleInfoModel>>> collector) throws Exception {
        vehicleInfoModelHashMap = stringVehicleInfoModelHashMap;
    }
}
