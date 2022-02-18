package cn.itcast.streaming.function.flatmap;

import cn.itcast.entity.OnlineDataObj;
import cn.itcast.entity.VehicleInfoModel;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 对原始数据及车辆基础信息广播流数据进行拉宽操作
 */
public class VehicleInfoMapMysqlFunction  implements CoFlatMapFunction<OnlineDataObj, HashMap<String, VehicleInfoModel>, OnlineDataObj> {
    //定义车辆相关信息map对象
    HashMap<String, VehicleInfoModel> vehicleInfoHashMap = new HashMap<>();

    /**
     * 原始的数据
     * @param onlineDataObj
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap1(OnlineDataObj onlineDataObj, Collector<OnlineDataObj> collector) throws Exception {
        //根据vin获取到车辆的基础信息
        VehicleInfoModel vehicleInfoModel = vehicleInfoHashMap.getOrDefault(onlineDataObj.getVin(), null);
        if(vehicleInfoModel!=null){
            //车系
            onlineDataObj.setSeriesName(vehicleInfoModel.getSeriesName());
            //车型
            onlineDataObj.setModelName(vehicleInfoModel.getModelName());
            //年限
            onlineDataObj.setLiveTime(vehicleInfoModel.getLiveTime());
            //销售日期
            onlineDataObj.setSalesDate(vehicleInfoModel.getSalesDate());
            //车辆类型
            onlineDataObj.setCarType(vehicleInfoModel.getCarType());

            //返回拉宽后的结果
            collector.collect(onlineDataObj);
        }else {
            System.out.println("原始数据中的vin："+onlineDataObj.getVin()+"，在车辆基础信息表中不存在...");
        }
    }

    /**
     * 车辆基础表的广播流数据
     * @param stringVehicleInfoModelHashMap
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap2(HashMap<String, VehicleInfoModel> stringVehicleInfoModelHashMap, Collector<OnlineDataObj> collector) throws Exception {
        this.vehicleInfoHashMap = stringVehicleInfoModelHashMap;
    }
}
