package cn.itcast.streaming.function.flatmap;

import cn.itcast.entity.ElectricFenceModel;
import cn.itcast.entity.ElectricFenceResultTmp;
import cn.itcast.entity.ItcastDataObj;
import cn.itcast.utils.DateUtil;
import cn.itcast.utils.DistanceCaculateUtil;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 自定义coFlatMapFuncyion的函数对象，实现原始数据与电子围栏广播流数据进行数据的合并，返回电子围栏规则模型流数据
 */
public class ElectricFenceRulesFuntion implements CoFlatMapFunction<ItcastDataObj, HashMap<String, ElectricFenceResultTmp>, ElectricFenceModel> {
    //定义电子围栏广播流数据的对象
    HashMap<String, ElectricFenceResultTmp> electricFenceResult = null;

    /**
     * 作用于原始数据的flatMap操作
     * @param itcastDataObj
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap1(ItcastDataObj itcastDataObj, Collector<ElectricFenceModel> collector) throws Exception {
        //定义需要返回的javaBean对象
        ElectricFenceModel electricFenceModel = new  ElectricFenceModel();
        //验证原始数据是否存在问题（车辆所在的经度、车辆所在的维度、以及车辆GpsTime）
        if(itcastDataObj.getLng() != 0 && itcastDataObj.getLat() != 0 &&
                itcastDataObj.getLng() != -999999D && itcastDataObj.getLat() != -999999D &&
                !itcastDataObj.getGpsTime().isEmpty()){
            //数据是正常的
            //获取到车架号
            String vin = itcastDataObj.getVin();
            ElectricFenceResultTmp electricFenceResultTmp = electricFenceResult.getOrDefault(vin, null);
            if(electricFenceResultTmp!= null){
                //该车辆关联了电子围栏规则（车辆被监控了）
                //当前上报的数据是否在电子围栏规则的生效时间期间内
                //获取到当前上报数据的地理位置时间
                long gpsTimestamp = DateUtil.convertStringToDate(itcastDataObj.getGpsTime()).getTime();
                if(electricFenceResultTmp.getStartTime().getTime() <= gpsTimestamp
                        &&  gpsTimestamp <= electricFenceResultTmp.getEndTime().getTime()){
                    //车辆上报的数据在电子围栏生效期内
                    electricFenceModel.setVin(itcastDataObj.getVin());
                    electricFenceModel.setGpsTime(itcastDataObj.getGpsTime());
                    electricFenceModel.setLng(itcastDataObj.getLng());
                    electricFenceModel.setLat(itcastDataObj.getLat());
                    electricFenceModel.setTerminalTime(itcastDataObj.getTerminalTime());
                    electricFenceModel.setTerminalTimestamp(itcastDataObj.getTerminalTimeStamp());
                    //电子围栏id
                    electricFenceModel.setEleId(electricFenceResultTmp.getId());
                    electricFenceModel.setEleName(electricFenceResultTmp.getName());
                    electricFenceModel.setAddress(electricFenceResultTmp.getAddress());
                    electricFenceModel.setRadius(electricFenceResultTmp.getRadius());
                    //电子围栏经度
                    electricFenceModel.setLongitude(electricFenceResultTmp.getLongitude());
                    electricFenceModel.setLatitude(electricFenceResultTmp.getLatitude());

                    //todo 如何计算当前的车辆处于电子围栏内还是电子围栏外，
                    // 需要根据车辆的经纬度于电子围栏中心点的经纬度进行距离计算，如果两点之间的距离大于半径的距离，即：电子围栏外，反之在电子围栏内
                    //单位是米
                    Double distance = DistanceCaculateUtil.getDistance(
                            electricFenceResultTmp.getLatitude(), electricFenceResultTmp.getLongitude(),
                            itcastDataObj.getLat(), itcastDataObj.getLng());
                    //判断是在电子围栏内还是电子围栏外
                    if(distance/ 1000 <= electricFenceResultTmp.getRadius()){
                        //电子围栏内
                        electricFenceModel.setNowStatus(0);
                    }else{
                        //电子围栏外
                        electricFenceModel.setNowStatus(1);
                    }
                    System.out.println("拉宽后的电子围栏分析模型结果数据："+electricFenceModel);

                    //返回数据
                    collector.collect(electricFenceModel);
                }else{
                    //该车辆没有关联任何的电子围栏规则（车辆没有被监控）
                    System.out.println("电子围栏id："+electricFenceResultTmp.getId()+"还没有生效，因此不需要后续的处理...");
                }
            } else {
                //该车辆没有关联任何的电子围栏规则（车辆没有被监控）
                System.out.println("vin："+vin+"， 没有符合的电子围栏规则，因此不需要后续的处理...");
            }
        }else{
            System.out.println("数据缺少：经度、维度、GpsTime字段："+itcastDataObj);
        }
    }

    /**
     * 作用域广播流数据的flatMap操作
     * 广播流的数据存储的是电子围栏规则及电子围栏规则关联的车辆数据，因此可以视为维度表
     * 因此可以认为维度表是原始数据拉宽的字段来源
     * @param electricFenceResultTmpHashMap
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap2(HashMap<String, ElectricFenceResultTmp> electricFenceResultTmpHashMap, Collector<ElectricFenceModel> collector) throws Exception {
        electricFenceResult = electricFenceResultTmpHashMap;
        System.out.println("广播流的电子围栏规则表的数据>>>"+electricFenceResult);
    }
}
