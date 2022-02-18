package cn.itcast.streaming.function.window;

import cn.itcast.entity.ItcastDataObj;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * 驾驶行程采样数据的自定义函数开发
 * 针对于驾驶行程数据（某个车辆15分钟内又没有上报数据的时候结束的窗口， 窗口内所有的数据进行按照5m、10m、15m、20m维度进行数据的采集和分析）
 * 继承自：RichWindowFunction抽象类
 *  ItcastDataObj：传入值类型
 *  String[]：返回值类型
 *  String：分组字段的类型：vin是字符串类型
 *  TimeWindow：窗口的类型，时间窗口EventTimeSessionWindow
 */
public class DriveSampleWindowFunction extends RichWindowFunction<ItcastDataObj, String[], String, TimeWindow> {
    /**
     * 重写apply方法，实现驾驶行程采样逻辑开发
     * @param key           分组字段
     * @param timeWindow    窗口类型
     * @param iterable      某个车辆的一个窗口（一个行程）内的所有的数据
     * @param collector     收集器，返回数据
     * @throws Exception
     */
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<ItcastDataObj> iterable, Collector<String[]> collector)
            throws Exception {
        System.out.println("驾驶行程采样自定义窗口业务开发");
        //根据分析，同一个窗口内的数据可能因为添加了水印，数据存在乱序的情况
        //todo 1：将迭代器转换成集合列表
        ArrayList<ItcastDataObj> itcastDataObjArrays = Lists.newArrayList(iterable);
        System.out.println("当前窗口的数据："+itcastDataObjArrays.size());
        //todo 2：对集合列表的数据进行排序操作
        itcastDataObjArrays.sort(((o1, o2) -> {
            //如果第一个元素对象的时间戳大于第二个元素的时间戳，升序排序
            if(o1.getTerminalTimeStamp() > o2.getTerminalTimeStamp()){
                return 1;
            }else if(o1.getTerminalTimeStamp() < o2.getTerminalTimeStamp()){
                return -1;
            }else{
                return 0;
            }
        }));
        //todo 3：首先要获取排序后的第一条数据作为周期的开始时间
        ItcastDataObj firstItcastDataObj = itcastDataObjArrays.get(0);
        System.out.println("第一条数据："+firstItcastDataObj);
        //采样的数据为5分钟、10分钟、15分钟、20分钟采样一次。
        // 采样数据的soc(剩余电量百分比)、mileage（累计里程）、speed（车速）、gps（经度+维度）、terminalTime（终端时间）字段属性需要拼接到一个字符串返回
        //soc(剩余电量百分比)
        StringBuffer singleSoc = new StringBuffer(String.valueOf(firstItcastDataObj.getSoc()));
        //mileage（累计里程）
        StringBuffer singleMileage = new StringBuffer(String.valueOf(firstItcastDataObj.getTotalOdometer()));
        //speed（车速）
        StringBuffer singleSpeed = new StringBuffer(String.valueOf(firstItcastDataObj.getVehicleSpeed()));
        //gps：地理位置
        StringBuffer singleGps = new StringBuffer(firstItcastDataObj.getLng() + "|" + firstItcastDataObj.getLat());
        //terminalTime：终端时间
        StringBuffer singleTerminalTime = new StringBuffer(String.valueOf(firstItcastDataObj.getTerminalTime()));
        //todo 4：获取排序后的最后一条数据作为当前窗口的最后一条数据
        ItcastDataObj lastItcastDataObj = itcastDataObjArrays.get(itcastDataObjArrays.size()-1);
        System.out.println("最后一条数据："+lastItcastDataObj);
        //todo 5：获取窗口的第一条数据的终端时间
        Long startTime = firstItcastDataObj.getTerminalTimeStamp();
        //todo 6：获取窗口的最后一条数据的终端时间
        Long endTime = lastItcastDataObj.getTerminalTimeStamp();
        //todo 7：遍历窗口内的每条数据
        for(ItcastDataObj itcastDataObj : itcastDataObjArrays){
            //获取到当前数据
            Long currentTimestamp = itcastDataObj.getTerminalTimeStamp();
            //todo 8：计算5m分钟采样周期内的数据
            if((currentTimestamp - startTime) < 5*1000*60 || currentTimestamp == endTime){
                singleSoc.append(","+itcastDataObj.getSoc());
                singleMileage.append(","+itcastDataObj.getTotalOdometer());
                singleSpeed.append(","+itcastDataObj.getVehicleSpeed());
                singleGps.append(","+itcastDataObj.getLng() + "|" + itcastDataObj.getLat());
                singleTerminalTime.append(","+itcastDataObj.getTerminalTime());
            }
            //重置startTime
            startTime = currentTimestamp;
        }

        String[] result = new String[7];
        //车辆唯一编号
        result[0] = firstItcastDataObj.getVin();
        //终端时间
        result[1] = String.valueOf(firstItcastDataObj.getTerminalTimeStamp());
        //剩余电量百分比
        result[2] = String.valueOf(singleSoc);
        //总里程数
        result[3] = String.valueOf(singleMileage);
        //车速
        result[4] = String.valueOf(singleSpeed);
        //地理位置
        result[5] = String.valueOf(singleGps);
        //终端时间
        result[6] = String.valueOf(singleTerminalTime);

        //返回数据
        collector.collect(result);
    }
}
