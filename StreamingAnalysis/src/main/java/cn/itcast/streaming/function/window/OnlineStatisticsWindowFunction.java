package cn.itcast.streaming.function.window;

import cn.itcast.entity.ItcastDataPartObj;
import cn.itcast.entity.OnlineDataObj;
import cn.itcast.utils.DateUtil;
import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * 在线远程实时故障分析自定义窗口操作
 */
public class OnlineStatisticsWindowFunction implements WindowFunction<ItcastDataPartObj, OnlineDataObj, String, TimeWindow> {
    /**
     * 自定义窗口操作
     * @param key
     * @param timeWindow
     * @param iterable
     * @param collector
     * @throws Exception
     */
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<ItcastDataPartObj> iterable, Collector<OnlineDataObj> collector) throws Exception {
        System.out.println("应用自定义窗口函数");
        //todo 1：将迭代器中的数据转换成本地集合对象
        ArrayList<ItcastDataPartObj> dataPartObjArrayList = Lists.newArrayList(iterable);
        dataPartObjArrayList.sort(((o1, o2) -> {
            //如果第一个元素对象的时间戳大于第二个元素的时间戳，升序排序
            if(o1.getTerminalTimeStamp() > o2.getTerminalTimeStamp()){
                return 1;
            }else if(o1.getTerminalTimeStamp() < o2.getTerminalTimeStamp()){
                return -1;
            }else{
                return 0;
            }
        }));
        //todo 2：获取开合中的第一条数据
        ItcastDataPartObj firstItcastDataPartObj = dataPartObjArrayList.get(0);

        //todo 3：循环遍历集合中的每条数据，将集合中存在异常指标的的数据过滤出来进行拼接到指定属性中存储
        //30s一个窗口，意味着每个车辆最多在该窗口中存在6条数据，因此6条中每条数据需要监测19个字段，如果19个字段中存在异常字段则进行字符串的拼接
        for (ItcastDataPartObj dataPartObj : dataPartObjArrayList){
            //监测当前这条数据中是否存在异常的字段
            if(filterNoAlarm(dataPartObj)){
                //没有报警字段
                OnlineDataObj onlineDataObj = setOnlineDataObj(dataPartObj, firstItcastDataPartObj, 0);
                System.out.println("没有异常指标数据");
                collector.collect(onlineDataObj);
            }else{
                OnlineDataObj onlineDataObj = setOnlineDataObj(dataPartObj, firstItcastDataPartObj, 1);
                System.out.println("存在异常指标数据");
                collector.collect(onlineDataObj);
            }
        }
    }

    /**
     * 将itcastDataPartObj转换成OnlineDataObj对象
     * @param dataPartObj
     * @param firstItcastDataPartObj
     * @param isAlarm
     * @return
     */
    private OnlineDataObj setOnlineDataObj(ItcastDataPartObj dataPartObj, ItcastDataPartObj firstItcastDataPartObj, int isAlarm) throws InvocationTargetException, IllegalAccessException {
        //定义需要返回的javaBean对象
        OnlineDataObj onlineDataObj = new OnlineDataObj();
//        onlineDataObj.setVin(dataPartObj.getVin());
//        onlineDataObj.setLat(dataPartObj.getLat());
//        onlineDataObj.setLng(dataPartObj.getLng());
//        onlineDataObj.setMileage(dataPartObj.getTotalOdometer());
//        onlineDataObj.setIsAlarm(isAlarm);
//        onlineDataObj.setAlarmName(String.join("~", addAlarmNameList(dataPartObj)));
//        onlineDataObj.setTerminalTime(dataPartObj.getTerminalTime());
//        onlineDataObj.setEarliestTime(firstItcastDataPartObj.getTerminalTime());
//        onlineDataObj.setMaxVoltageBattery(dataPartObj.getMaxVoltageBattery());
//        onlineDataObj.setMinVoltageBattery(dataPartObj.getMinVoltageBattery());
//        onlineDataObj.setMaxTemperatureValue(dataPartObj.getMaxTemperatureValue());
//        onlineDataObj.setMinTemperatureValue(dataPartObj.getMinTemperatureValue());
//        onlineDataObj.setSpeed(dataPartObj.getSpeed());
//        onlineDataObj.setSoc(dataPartObj.getSoc());
//        onlineDataObj.setChargeFlag(getChargeState(dataPartObj.getChargeStatus()));
//        onlineDataObj.setTotalVoltage(dataPartObj.getTotalVoltage());
//        onlineDataObj.setTotalCurrent(dataPartObj.getTotalCurrent());
//        onlineDataObj.setBatteryVoltage(dataPartObj.getBatteryVoltage());
//        onlineDataObj.setProbeTemperatures(dataPartObj.getProbeTemperatures());
//        onlineDataObj.setProcessTime(DateUtil.getCurrentDateTime());

        //将dataPartObj的属性赋值给onlineDataObj
        BeanUtils.copyProperties(onlineDataObj, dataPartObj);
        onlineDataObj.setMileage(dataPartObj.getTotalOdometer());
        onlineDataObj.setIsAlarm(isAlarm);
        onlineDataObj.setAlarmName(String.join("~", addAlarmNameList(dataPartObj)));
        onlineDataObj.setEarliestTime(firstItcastDataPartObj.getTerminalTime());
        onlineDataObj.setChargeFlag(getChargeState(dataPartObj.getChargeStatus()));
        onlineDataObj.setProcessTime(DateUtil.getCurrentDateTime());

        //返回处理后的数据
        return onlineDataObj;
    }


    /**
     * 根据充电状态返回充电标记
     * @param chargeState
     * @return
     */
    private Integer getChargeState(Integer chargeState){
        int chargeFlag = -999999;//充电状态的初始值
        //充电状态：0x01: 停车充电、 0x02: 行车充电
        if(chargeState == 1 || chargeState==2){
            chargeFlag = 1;
        }
        //0x04:充电完成 0x03: 未充电
        else if(chargeState == 4 || chargeState == 3){
            chargeFlag = 0;
        }else{
            chargeFlag = 2;
        }
        return chargeFlag;
    }

    /**
     * 将每条数据的故障名称追加到故障名称列表中
     * @return
     */
    private ArrayList<String> addAlarmNameList(ItcastDataPartObj dataPartObj){
        //定义故障名称列表对象
        ArrayList<String> alarmNameList = new ArrayList<>();
        //电池高温报警
        if(dataPartObj.getBatteryAlarm() == 1) {
            alarmNameList.add("电池高温报警");
        }
        //单体电池高压报警
        if(dataPartObj.getSingleBatteryOverVoltageAlarm() == 1) {
            alarmNameList.add("单体电池高压报警");
        }
        //电池单体一致性差报警
        if(dataPartObj.getBatteryConsistencyDifferenceAlarm() == 1) {
            alarmNameList.add("电池单体一致性差报警");
        }
        //绝缘报警
        if(dataPartObj.getInsulationAlarm() == 1) {
            alarmNameList.add("绝缘报警");
        }
        //高压互锁状态报警
        if(dataPartObj.getHighVoltageInterlockStateAlarm() == 1) {
            alarmNameList.add("高压互锁状态报警");
        }
        //SOC跳变报警
        if(dataPartObj.getSocJumpAlarm() == 1) {
            alarmNameList.add("SOC跳变报警");
        }
        //驱动电机控制器温度报警
        if(dataPartObj.getDriveMotorControllerTemperatureAlarm() == 1) {
            alarmNameList.add("驱动电机控制器温度报警");
        }
        //DC-DC温度报警（dc-dc可以理解为车辆动力智能系统转换器）
        if(dataPartObj.getDcdcTemperatureAlarm() == 1) {
            alarmNameList.add("DC-DC温度报警");
        }
        //SOC过高报警
        if(dataPartObj.getSocHighAlarm() == 1) {
            alarmNameList.add("SOC过高报警");
        }
        //SOC低报警
        if(dataPartObj.getSocLowAlarm() == 1) {
            alarmNameList.add("SOC低报警");
        }
        //温度差异报警
        if(dataPartObj.getTemperatureDifferenceAlarm() == 1) {
            alarmNameList.add("温度差异报警");
        }
        //车载储能装置欠压报警
        if(dataPartObj.getVehicleStorageDeviceUndervoltageAlarm() == 1) {
            alarmNameList.add("车载储能装置欠压报警");
        }
        //DC-DC状态报警
        if(dataPartObj.getDcdcStatusAlarm() == 1) {
            alarmNameList.add("DC-DC状态报警");
        }
        //单体电池欠压报警
        if(dataPartObj.getSingleBatteryUnderVoltageAlarm() == 1) {
            alarmNameList.add("单体电池欠压报警");
        }
        //可充电储能系统不匹配报警
        if(dataPartObj.getRechargeableStorageDeviceMismatchAlarm() == 1) {
            alarmNameList.add("可充电储能系统不匹配报警");
        }
        //车载储能装置过压报警
        if(dataPartObj.getVehicleStorageDeviceOvervoltageAlarm() == 1) {
            alarmNameList.add("车载储能装置过压报警");
        }
        //制动系统报警
        if(dataPartObj.getBrakeSystemAlarm() == 1) {
            alarmNameList.add("制动系统报警");
        }
        //驱动电机温度报警
        if(dataPartObj.getDriveMotorTemperatureAlarm() == 1) {
            alarmNameList.add("驱动电机温度报警");
        }
        //车载储能装置类型过充报警
        if(dataPartObj.getVehiclePureDeviceTypeOvercharge() == 1) {
            alarmNameList.add("车载储能装置类型过充报警");
        }
        return alarmNameList;
    }

    /**
     * 判断是否存在报警的字段
     * @param dataPartObj
     * @return
     */
    private boolean filterNoAlarm(ItcastDataPartObj dataPartObj) {
        //电池高温报警
        if((dataPartObj.getBatteryAlarm() == 1) ||
                //单体电池高压报警
                dataPartObj.getSingleBatteryOverVoltageAlarm() == 1 ||
                //电池单体一致性差报警
                dataPartObj.getBatteryConsistencyDifferenceAlarm() == 1 ||
                //绝缘报警
                dataPartObj.getInsulationAlarm() == 1 ||
                //高压互锁状态报警
                dataPartObj.getHighVoltageInterlockStateAlarm() == 1 ||
                //SOC跳变报警
                dataPartObj.getSocJumpAlarm() == 1 ||
                //驱动电机控制器温度报警
                dataPartObj.getDriveMotorControllerTemperatureAlarm() == 1 ||
                //DC-DC温度报警（dc-dc可以理解为车辆动力智能系统转换器）
                dataPartObj.getDcdcTemperatureAlarm() ==1 ||
                //SOC过高报警
                dataPartObj.getSocHighAlarm() == 1||
                //SOC低报警
                dataPartObj.getSocLowAlarm() == 1 ||
                //温度差异报警
                dataPartObj.getTemperatureDifferenceAlarm() == 1||
                //车载储能装置欠压报警
                dataPartObj.getVehicleStorageDeviceUndervoltageAlarm() == 1||
                //DC-DC状态报警
                dataPartObj.getDcdcStatusAlarm() == 1||
                //单体电池欠压报警
                dataPartObj.getSingleBatteryUnderVoltageAlarm() == 1||
                //可充电储能系统不匹配报警
                dataPartObj.getRechargeableStorageDeviceMismatchAlarm() == 1||
                //车载储能装置过压报警
                dataPartObj.getVehicleStorageDeviceOvervoltageAlarm() == 1||
                //制动系统报警
                dataPartObj.getBrakeSystemAlarm() == 1 ||
                //驱动电机温度报警
                dataPartObj.getDriveMotorTemperatureAlarm() == 1 ||
                //车载储能装置类型过充报警
                dataPartObj.getVehiclePureDeviceTypeOvercharge() == 1
        )
            return  false;
        else
            return true;
    }
}
