package cn.itcast.streaming.task;

import cn.itcast.entity.ElectricFenceModel;
import cn.itcast.entity.ElectricFenceResultTmp;
import cn.itcast.entity.ItcastDataObj;
import cn.itcast.streaming.function.flatmap.ElectricFenceModelFunction;
import cn.itcast.streaming.function.flatmap.ElectricFenceRulesFuntion;
import cn.itcast.streaming.function.window.ElectricFenceWindowFunction;
import cn.itcast.streaming.sink.ElectricFenceMysqlSink;
import cn.itcast.streaming.source.MysqlElectricFenceResultSource;
import cn.itcast.streaming.source.MysqlElectricFenceSouce;
import cn.itcast.streaming.watermark.ElectricFenceWatermark;
import cn.itcast.utils.JsonParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;

/**
 * 电子围栏实时业务分析
 * 消费kafka数据，将消费出来的数据进行处理以后，实时的写入到Mysql中
 *
 * 测试数据：
 *
 * {"gearDriveForce":0,"batteryConsistencyDifferenceAlarm":0,"soc":94,"socJumpAlarm":0,"satNum":10,"caterpillaringFunction":0,"socLowAlarm":0,"chargingGunConnectionState":0,"minTemperatureSubSystemNum":1,"chargedElectronicLockStatus":0,"terminalTime":"2019-11-20 15:00:00","maxVoltageBatteryNum":49,"singleBatteryOverVoltageAlarm":0,"otherFaultCount":0,"vehicleStorageDeviceOvervoltageAlarm":0,"brakeSystemAlarm":0,"serverTime":"2019-11-20 15:34:00.154","vin":"LS6A2E0E3KA006811","rechargeableStorageDevicesFaultCount":0,"driveMotorTemperatureAlarm":0,"remainedPowerMile":0,"dcdcStatusAlarm":0,"gearBrakeForce":1,"lat":39.9134,"driveMotorFaultCodes":"","vehicleSpeed":0.0,"lng":116.412752,"gpsTime":"2019-11-20 15:33:41","nevChargeSystemVoltageDtoList":[{"currentBatteryStartNum":1,"batteryVoltage":[4.15,4.15,4.147,4.148,4.149,4.148,4.139,4.147,4.146,4.152,4.149,4.149,4.15,4.149,4.146,4.148,4.152,4.147,4.149,4.15,4.146,4.15,4.149,4.148,4.148,4.149,4.148,4.149,4.152,4.141,4.146,4.151,4.152,4.154,4.15,4.147,4.15,4.144,4.146,4.145,4.149,4.148,4.147,4.148,4.144,4.143,4.147,4.141,4.156,4.155,4.15,4.15,4.151,4.156,4.153,4.145,4.151,4.144,4.15,4.152,4.145,4.15,4.148,4.149,4.151,4.156,4.152,4.152,4.151,4.142,4.149,4.151,4.148,4.145,4.148,4.146,4.148,4.146,4.151,4.138,4.147,4.138,4.146,4.142,4.149,4.15,4.146,4.148,4.143,4.146,4.147,4.147,4.155,4.151,4.141,4.147],"chargeSystemVoltage":398.2,"currentBatteryCount":96,"batteryCount":96,"childSystemNum":1,"chargeSystemCurrent":0.2999878}],"engineFaultCount":0,"currentElectricity":94,"singleBatteryUnderVoltageAlarm":0,"maxVoltageBatterySubSystemNum":1,"minTemperatureProbe":13,"driveMotorNum":1,"totalVoltage":398.2,"maxAlarmLevel":0,"temperatureDifferenceAlarm":0,"averageEnergyConsumption":0.0,"minVoltageBattery":4.138,"driveMotorData":[{"controllerInputVoltage":399.0,"controllerTemperature":38,"revolutionSpeed":0,"num":1,"controllerDcBusCurrent":0.0,"length":0,"temperature":40,"torque":0.0,"state":4,"type":0,"MAX_BYTE_VALUE":127}],"shiftPositionStatus":0,"minVoltageBatteryNum":80,"engineFaultCodes":"","minTemperatureValue":17,"chargeStatus":3,"deviceTime":"2019-11-20 15:33:41","shiftPosition":0,"totalOdometer":38595.0,"alti":57.0,"speed":0.0,"socHighAlarm":0,"vehicleStorageDeviceUndervoltageAlarm":0,"totalCurrent":0.3,"batteryAlarm":0,"rechargeableStorageDeviceMismatchAlarm":0,"isHistoryPoi":0,"maxVoltageBattery":4.156,"vehiclePureDeviceTypeOvercharge":0,"dcdcTemperatureAlarm":0,"isValidGps":true,"lastUpdatedTime":"2019-11-20 15:34:00.154","driveMotorControllerTemperatureAlarm":0,"nevChargeSystemTemperatureDtoList":[{"probeTemperatures":[18,18,18,20,19,19,19,20,19,19,18,19,17,17,17,17,17,17,18,18,18,17,18,18,17,17,17,17,17,17,18,18],"chargeTemperatureProbeNum":32,"childSystemNum":1}],"igniteCumulativeMileage":0.0,"dcStatus":1,"maxTemperatureSubSystemNum":1,"carStatus":2,"minVoltageBatterySubSystemNum":1,"heading":2.68,"driveMotorFaultCount":0,"tuid":"50003001190517140000000518553162","energyRecoveryStatus":0,"targetType":"VehicleRealtimeDto","maxTemperatureProbe":4,"rechargeableStorageDevicesFaultCodes":"","carMode":1,"highVoltageInterlockStateAlarm":0,"insulationAlarm":0,"maxTemperatureValue":20,"otherFaultCodes":"","remainPower":94.00001,"insulateResistance":6417,"batteryLowTemperatureHeater":0}
 * {"gearDriveForce":0,"batteryConsistencyDifferenceAlarm":0,"soc":94,"socJumpAlarm":0,"satNum":10,"caterpillaringFunction":0,"socLowAlarm":0,"chargingGunConnectionState":0,"minTemperatureSubSystemNum":1,"chargedElectronicLockStatus":0,"terminalTime":"2019-11-20 15:02:00","maxVoltageBatteryNum":49,"singleBatteryOverVoltageAlarm":0,"otherFaultCount":0,"vehicleStorageDeviceOvervoltageAlarm":0,"brakeSystemAlarm":0,"serverTime":"2019-11-20 15:34:00.154","vin":"LS6A2E0E3KA006811","rechargeableStorageDevicesFaultCount":0,"driveMotorTemperatureAlarm":0,"remainedPowerMile":0,"dcdcStatusAlarm":0,"gearBrakeForce":1,"lat":39.778492,"driveMotorFaultCodes":"","vehicleSpeed":0.0,"lng":116.461678,"gpsTime":"2019-11-20 15:33:41","nevChargeSystemVoltageDtoList":[{"currentBatteryStartNum":1,"batteryVoltage":[4.15,4.15,4.147,4.148,4.149,4.148,4.139,4.147,4.146,4.152,4.149,4.149,4.15,4.149,4.146,4.148,4.152,4.147,4.149,4.15,4.146,4.15,4.149,4.148,4.148,4.149,4.148,4.149,4.152,4.141,4.146,4.151,4.152,4.154,4.15,4.147,4.15,4.144,4.146,4.145,4.149,4.148,4.147,4.148,4.144,4.143,4.147,4.141,4.156,4.155,4.15,4.15,4.151,4.156,4.153,4.145,4.151,4.144,4.15,4.152,4.145,4.15,4.148,4.149,4.151,4.156,4.152,4.152,4.151,4.142,4.149,4.151,4.148,4.145,4.148,4.146,4.148,4.146,4.151,4.138,4.147,4.138,4.146,4.142,4.149,4.15,4.146,4.148,4.143,4.146,4.147,4.147,4.155,4.151,4.141,4.147],"chargeSystemVoltage":398.2,"currentBatteryCount":96,"batteryCount":96,"childSystemNum":1,"chargeSystemCurrent":0.2999878}],"engineFaultCount":0,"currentElectricity":94,"singleBatteryUnderVoltageAlarm":0,"maxVoltageBatterySubSystemNum":1,"minTemperatureProbe":13,"driveMotorNum":1,"totalVoltage":398.2,"maxAlarmLevel":0,"temperatureDifferenceAlarm":0,"averageEnergyConsumption":0.0,"minVoltageBattery":4.138,"driveMotorData":[{"controllerInputVoltage":399.0,"controllerTemperature":38,"revolutionSpeed":0,"num":1,"controllerDcBusCurrent":0.0,"length":0,"temperature":40,"torque":0.0,"state":4,"type":0,"MAX_BYTE_VALUE":127}],"shiftPositionStatus":0,"minVoltageBatteryNum":80,"engineFaultCodes":"","minTemperatureValue":17,"chargeStatus":3,"deviceTime":"2019-11-20 15:33:41","shiftPosition":0,"totalOdometer":38595.0,"alti":57.0,"speed":0.0,"socHighAlarm":0,"vehicleStorageDeviceUndervoltageAlarm":0,"totalCurrent":0.3,"batteryAlarm":0,"rechargeableStorageDeviceMismatchAlarm":0,"isHistoryPoi":0,"maxVoltageBattery":4.156,"vehiclePureDeviceTypeOvercharge":0,"dcdcTemperatureAlarm":0,"isValidGps":true,"lastUpdatedTime":"2019-11-20 15:34:00.154","driveMotorControllerTemperatureAlarm":0,"nevChargeSystemTemperatureDtoList":[{"probeTemperatures":[18,18,18,20,19,19,19,20,19,19,18,19,17,17,17,17,17,17,18,18,18,17,18,18,17,17,17,17,17,17,18,18],"chargeTemperatureProbeNum":32,"childSystemNum":1}],"igniteCumulativeMileage":0.0,"dcStatus":1,"maxTemperatureSubSystemNum":1,"carStatus":2,"minVoltageBatterySubSystemNum":1,"heading":2.68,"driveMotorFaultCount":0,"tuid":"50003001190517140000000518553162","energyRecoveryStatus":0,"targetType":"VehicleRealtimeDto","maxTemperatureProbe":4,"rechargeableStorageDevicesFaultCodes":"","carMode":1,"highVoltageInterlockStateAlarm":0,"insulationAlarm":0,"maxTemperatureValue":20,"otherFaultCodes":"","remainPower":94.00001,"insulateResistance":6417,"batteryLowTemperatureHeater":0}
 * {"gearDriveForce":0,"batteryConsistencyDifferenceAlarm":0,"soc":94,"socJumpAlarm":0,"satNum":10,"caterpillaringFunction":0,"socLowAlarm":0,"chargingGunConnectionState":0,"minTemperatureSubSystemNum":1,"chargedElectronicLockStatus":0,"terminalTime":"2019-11-20 15:04:00","maxVoltageBatteryNum":49,"singleBatteryOverVoltageAlarm":0,"otherFaultCount":0,"vehicleStorageDeviceOvervoltageAlarm":0,"brakeSystemAlarm":0,"serverTime":"2019-11-20 15:34:00.154","vin":"LS6A2E0E3KA006811","rechargeableStorageDevicesFaultCount":0,"driveMotorTemperatureAlarm":0,"remainedPowerMile":0,"dcdcStatusAlarm":0,"gearBrakeForce":1,"lat":39.778492,"driveMotorFaultCodes":"","vehicleSpeed":0.0,"lng":116.461678,"gpsTime":"2019-11-20 15:33:41","nevChargeSystemVoltageDtoList":[{"currentBatteryStartNum":1,"batteryVoltage":[4.15,4.15,4.147,4.148,4.149,4.148,4.139,4.147,4.146,4.152,4.149,4.149,4.15,4.149,4.146,4.148,4.152,4.147,4.149,4.15,4.146,4.15,4.149,4.148,4.148,4.149,4.148,4.149,4.152,4.141,4.146,4.151,4.152,4.154,4.15,4.147,4.15,4.144,4.146,4.145,4.149,4.148,4.147,4.148,4.144,4.143,4.147,4.141,4.156,4.155,4.15,4.15,4.151,4.156,4.153,4.145,4.151,4.144,4.15,4.152,4.145,4.15,4.148,4.149,4.151,4.156,4.152,4.152,4.151,4.142,4.149,4.151,4.148,4.145,4.148,4.146,4.148,4.146,4.151,4.138,4.147,4.138,4.146,4.142,4.149,4.15,4.146,4.148,4.143,4.146,4.147,4.147,4.155,4.151,4.141,4.147],"chargeSystemVoltage":398.2,"currentBatteryCount":96,"batteryCount":96,"childSystemNum":1,"chargeSystemCurrent":0.2999878}],"engineFaultCount":0,"currentElectricity":94,"singleBatteryUnderVoltageAlarm":0,"maxVoltageBatterySubSystemNum":1,"minTemperatureProbe":13,"driveMotorNum":1,"totalVoltage":398.2,"maxAlarmLevel":0,"temperatureDifferenceAlarm":0,"averageEnergyConsumption":0.0,"minVoltageBattery":4.138,"driveMotorData":[{"controllerInputVoltage":399.0,"controllerTemperature":38,"revolutionSpeed":0,"num":1,"controllerDcBusCurrent":0.0,"length":0,"temperature":40,"torque":0.0,"state":4,"type":0,"MAX_BYTE_VALUE":127}],"shiftPositionStatus":0,"minVoltageBatteryNum":80,"engineFaultCodes":"","minTemperatureValue":17,"chargeStatus":3,"deviceTime":"2019-11-20 15:33:41","shiftPosition":0,"totalOdometer":38595.0,"alti":57.0,"speed":0.0,"socHighAlarm":0,"vehicleStorageDeviceUndervoltageAlarm":0,"totalCurrent":0.3,"batteryAlarm":0,"rechargeableStorageDeviceMismatchAlarm":0,"isHistoryPoi":0,"maxVoltageBattery":4.156,"vehiclePureDeviceTypeOvercharge":0,"dcdcTemperatureAlarm":0,"isValidGps":true,"lastUpdatedTime":"2019-11-20 15:34:00.154","driveMotorControllerTemperatureAlarm":0,"nevChargeSystemTemperatureDtoList":[{"probeTemperatures":[18,18,18,20,19,19,19,20,19,19,18,19,17,17,17,17,17,17,18,18,18,17,18,18,17,17,17,17,17,17,18,18],"chargeTemperatureProbeNum":32,"childSystemNum":1}],"igniteCumulativeMileage":0.0,"dcStatus":1,"maxTemperatureSubSystemNum":1,"carStatus":2,"minVoltageBatterySubSystemNum":1,"heading":2.68,"driveMotorFaultCount":0,"tuid":"50003001190517140000000518553162","energyRecoveryStatus":0,"targetType":"VehicleRealtimeDto","maxTemperatureProbe":4,"rechargeableStorageDevicesFaultCodes":"","carMode":1,"highVoltageInterlockStateAlarm":0,"insulationAlarm":0,"maxTemperatureValue":20,"otherFaultCodes":"","remainPower":94.00001,"insulateResistance":6417,"batteryLowTemperatureHeater":0}
 */
public class ElectricFenceTask extends BaseTask {

    /**
     * 入口方法
     * @param args
     */
    public static void main(String[] args) {
        /**
         * 实现步骤:
         * 1）初始化flink流处理的运行环境（设置按照事件时间处理数据、设置hadoopHome的用户名、设置checkpoint）
         * 2）读取kafka数据源（调用父类的方法）
         * 3）将字符串转换成javaBean（ItcastDataObj）对象
         * 4）过滤出来正常数据
         * 5）读取电子围栏规则数据以及电子围栏规则关联的车辆数据并进行广播
         * 6）将原始数据（消费的kafka数据）与电子围栏规则数据进行关联操作（Connect）
         * 7）对合并后的数据分组后应用90s滚动窗口，然后对窗口进行自定义函数的开发（计算出来该窗口的数据属于电子围栏外还是电子围栏内）
         * 8）读取电子围栏分析结果表的数据并进行广播
         * 9）对第七步和第八步产生的数据进行关联操作（connect）
         * 10）对第九步的结果进行滚动窗口操作，应用自定义窗口函数（实现添加uuid和inMysql属性赋值）
         * 11）将分析后的电子围栏结果数据实时写入到mysql数据库中
         * 12）运行作业，等待停止
         */

        //TODO 1）初始化flink流处理的运行环境（设置按照事件时间处理数据、设置hadoopHome的用户名、设置checkpoint）
        StreamExecutionEnvironment env = getEnv(ElectricFenceTask.class.getSimpleName());

        //TODO 2）读取kafka数据源（调用父类的方法）
        DataStream<String> kafkaStream = createKafkaStream(SimpleStringSchema.class);
        kafkaStream.print("消费到的原始数据>>>");

        //TODO 3）将字符串转换成javaBean（ItcastDataObj）对象
        SingleOutputStreamOperator<ItcastDataObj> itcastDataObjStream = kafkaStream.map(JsonParseUtil::parseJsonToObject);

        //TODO 4）过滤出来正常数据
        SingleOutputStreamOperator<ItcastDataObj> srcItcastDataObjStream = itcastDataObjStream.filter(
                itcastDataObj -> StringUtils.isEmpty(itcastDataObj.getErrorData()));

        //TODO 5）读取电子围栏规则数据以及电子围栏规则关联的车辆数据并进行广播
        DataStream<HashMap<String, ElectricFenceResultTmp>> electricFenceVinsStream = env.addSource(new MysqlElectricFenceSouce()).broadcast();

        //TODO 6）将原始数据（消费的kafka数据）与电子围栏规则数据进行关联操作（Connect）
        ConnectedStreams<ItcastDataObj, HashMap<String, ElectricFenceResultTmp>> electricFenceVinsConnectStream =
                srcItcastDataObjStream.connect(electricFenceVinsStream);

        SingleOutputStreamOperator<ElectricFenceModel> electricFenceModelStream = electricFenceVinsConnectStream.flatMap(new ElectricFenceRulesFuntion());
        electricFenceModelStream.print("原始数据于电子围栏规则广播流数据拉宽后的结果>>>");

        //TODO 7）对合并后的数据分组后应用90s滚动窗口，然后对窗口进行自定义函数的开发（计算出来该窗口的数据属于电子围栏外还是电子围栏内）
        SingleOutputStreamOperator<ElectricFenceModel> electricFenceDataStream = electricFenceModelStream
                .assignTimestampsAndWatermarks(new ElectricFenceWatermark())
                .keyBy(ElectricFenceModel::getVin) //根据车架号进行分流操作
                .window(TumblingEventTimeWindows.of(Time.seconds(90))) //设置90s滚动一次窗口
                .apply(new ElectricFenceWindowFunction());
        electricFenceDataStream.printToErr("验证进或者出电子围栏分析结果>>>");

        //TODO 8）读取电子围栏分析结果表的数据并进行广播
        DataStream<HashMap<String, Long>> electricFenceResultDataStream = env.addSource(new MysqlElectricFenceResultSource()).broadcast();

        //TODO 9）对第七步和第八步产生的数据进行关联操作（connect）
        ConnectedStreams<ElectricFenceModel, HashMap<String, Long>> connectedStreams = electricFenceDataStream.connect(electricFenceResultDataStream);

        //TODO 10）对第九步的结果进行滚动窗口操作，应用自定义窗口函数（实现添加uuid和inMysql属性赋值）
        SingleOutputStreamOperator<ElectricFenceModel> resultDataStream = connectedStreams.flatMap(new ElectricFenceModelFunction());

        //TODO 11）将分析后的电子围栏结果数据实时写入到mysql数据库中
        resultDataStream.addSink(new ElectricFenceMysqlSink());

        try {
            //TODO 12）运行作业，等待停止
            env.execute();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
