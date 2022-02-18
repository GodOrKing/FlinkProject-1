package cn.itcast.streaming.sink;

import cn.itcast.entity.ItcastDataObj;
import cn.itcast.utils.DateUtil;
import cn.itcast.utils.StringUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 需求：将正常的数据实时的写入到hbase中（使用优化后的hbase写入方式写入数据）
 * 实现步骤：
 * 1）继承自RichSinkFunction对象
 * 2）重写父类的方法：
 *  2.1）open
 *      实现hbase的连接，然后获得table对象
 *  2.2）close
 *      释放资源，关闭hbase的连接
 *  2.3）invoke
 *      创建put实例，将数据一条条的写入到hbase表中
 */
public class SrcDataToHBaseSinkOptimizer extends RichSinkFunction<ItcastDataObj> {
    //定义日志操作对象
    private final static Logger logger = LoggerFactory.getLogger(SrcDataToHBaseSinkOptimizer.class);
    //定义hbase操作的connection对象
    private Connection connection;
    //定义需要操作的hbase的表名的属性
    private String tableName;
    //定义buffredMutator对象
    private BufferedMutator bufferedMutator;

    /**
     * 定义构造方法，接受传递过来的hbase的表名
     * @param tableName
     */
    public SrcDataToHBaseSinkOptimizer(String tableName){
        this.tableName = tableName;
    }

    /**
     * 初始化hbase的连接操作
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取到全局参数对象
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //1：定义hbase的连接配置对象
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", parameterTool.getRequired("zookeeper.quorum"));
        configuration.set("hbase.zookeeper.property.clientPort", parameterTool.getRequired("zookeeper.clientPort"));
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        //2：创建hbase的连接对象
        connection = ConnectionFactory.createConnection();
        //实例化table对象
        //table = connection.getTable(TableName.valueOf(tableName));
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
        bufferedMutator = connection.getBufferedMutator(params);
        logger.warn("获取hbase的连接对象，{}表对象初始化成功.", tableName);
        System.out.println("表初始化成功");
    }

    /**
     * 关闭hbase的连接，释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        //当调用close方法的时候，不验证当前缓冲区的大小是否满足了最大缓冲区的阈值，都会执行hbase的写入操作
        if(bufferedMutator != null) bufferedMutator.close();
        if(connection!=null) connection.close();
    }

    /**
     * 实现数据一条条的写入到hbase表的逻辑
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(ItcastDataObj value, Context context) throws Exception {
        System.out.println("需要写入的数据："+value);
        //实现每条数据实时的写入到hbase表中
        Put put = setDataSourcePut(value);
        //mutate：不强制刷新，必须要等到缓冲区大小的阈值满足了条件才会同步到hbase中
        //这个方法被执行的时候，意味着数据可能还没写完，因此需要判断当前缓冲区大小是否满足缓冲区最大的阈值，如果满足了最大的阈值才会将数据写入到hbase中
        bufferedMutator.mutate(put);

        //可以去掉，这句话的意思是强制的将缓冲区的数据同步到hbase中
        //下面这句话是强制刷新，在一些业务中假如数据产生的频率比较低，导致缓冲区写满的速度比较慢，因此导致hbase的数据延迟比较高
        //异步写入
        //使用场景，假如业务对延迟要求比较高，可以加上flush，每条数据到达缓存，都会强制异步到hbase中
        //假如业务对延迟要求比较低，可以不加flush，等到数据写满了缓冲区，在进行批量数据的同步到hbase中（延迟稍微高一些）
        bufferedMutator.flush();
    }

    /**
     * 根据itcastDataObj转换成put返回
     * @param itcastDataObj
     * @return
     */
    private Put setDataSourcePut(ItcastDataObj itcastDataObj) {
        //如何设计rowkey
        //将车架号作为rowKey的一部分：getVin，使用车架号作为rowKey违反了唯一原则，因为一个车辆会产生很多数据
        //需要将时间作为rowkey的另外一部分，根据vin和时间可以保证数据的唯一原则，但是如果加入过多的字段作为rowkey可能会违反长度原则
        //LS6A2E0E3KA006811+1617001119630
        //LS6A2E0E3KA006811+1617001184000，违反了散列原则
        //LS6A2E0E3KA006811+0369111007161
        //LS6A2E0E3KA006811+0004811007161
        String rowKey = itcastDataObj.getVin() + StringUtil.reverse(itcastDataObj.getTerminalTimeStamp().toString());
        //定义列族的名称
        String cf = "cf";
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vin"), Bytes.toBytes(itcastDataObj.getVin()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("terminalTime"), Bytes.toBytes(itcastDataObj.getTerminalTime()));
        if (itcastDataObj.getSoc() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("soc"), Bytes.toBytes(String.valueOf(itcastDataObj.getSoc())));
        if (itcastDataObj.getLat() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lat"), Bytes.toBytes(String.valueOf(itcastDataObj.getLat())));
        if (itcastDataObj.getLng() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lng"), Bytes.toBytes(String.valueOf(itcastDataObj.getLng())));
        if (itcastDataObj.getGearDriveForce() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("gearDriveForce"), Bytes.toBytes(String.valueOf(itcastDataObj.getGearDriveForce())));
        if (itcastDataObj.getBatteryConsistencyDifferenceAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("batteryConsistencyDifferenceAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getBatteryConsistencyDifferenceAlarm())));
        if (itcastDataObj.getSocJumpAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("socJumpAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getSocJumpAlarm())));
        if (itcastDataObj.getCaterpillaringFunction() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("caterpillaringFunction"), Bytes.toBytes(String.valueOf(itcastDataObj.getCaterpillaringFunction())));
        if (itcastDataObj.getSatNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("satNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getSatNum())));
        if (itcastDataObj.getSocLowAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("socLowAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getSocLowAlarm())));
        if (itcastDataObj.getChargingGunConnectionState() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("chargingGunConnectionState"), Bytes.toBytes(String.valueOf(itcastDataObj.getChargingGunConnectionState())));
        if (itcastDataObj.getMinTemperatureSubSystemNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minTemperatureSubSystemNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getMinTemperatureSubSystemNum())));
        if (itcastDataObj.getChargedElectronicLockStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("chargedElectronicLockStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getChargedElectronicLockStatus())));
        if (itcastDataObj.getMaxVoltageBatteryNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxVoltageBatteryNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getMaxVoltageBatteryNum())));
        if (itcastDataObj.getSingleBatteryOverVoltageAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("singleBatteryOverVoltageAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getSingleBatteryOverVoltageAlarm())));
        if (itcastDataObj.getOtherFaultCount() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("otherFaultCount"), Bytes.toBytes(String.valueOf(itcastDataObj.getOtherFaultCount())));
        if (itcastDataObj.getVehicleStorageDeviceOvervoltageAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vehicleStorageDeviceOvervoltageAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getVehicleStorageDeviceOvervoltageAlarm())));
        if (itcastDataObj.getBrakeSystemAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("brakeSystemAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getBrakeSystemAlarm())));
        if (!itcastDataObj.getServerTime().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("serverTime"), Bytes.toBytes(itcastDataObj.getServerTime()));
        if (itcastDataObj.getRechargeableStorageDevicesFaultCount() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("rechargeableStorageDevicesFaultCount"), Bytes.toBytes(String.valueOf(itcastDataObj.getRechargeableStorageDevicesFaultCount())));
        if (itcastDataObj.getDriveMotorTemperatureAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driveMotorTemperatureAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriveMotorTemperatureAlarm())));
        if (itcastDataObj.getGearBrakeForce() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("gearBrakeForce"), Bytes.toBytes(String.valueOf(itcastDataObj.getGearBrakeForce())));
        if (itcastDataObj.getDcdcStatusAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dcdcStatusAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getDcdcStatusAlarm())));
        if (!itcastDataObj.getDriveMotorFaultCodes().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driveMotorFaultCodes"), Bytes.toBytes(itcastDataObj.getDriveMotorFaultCodes()));
        if (!itcastDataObj.getDeviceType().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("deviceType"), Bytes.toBytes(itcastDataObj.getDeviceType()));
        if (itcastDataObj.getVehicleSpeed() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vehicleSpeed"), Bytes.toBytes(String.valueOf(itcastDataObj.getVehicleSpeed())));
        if (itcastDataObj.getChargingTimeExtensionReason() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("chargingTimeExtensionReason"), Bytes.toBytes(String.valueOf(itcastDataObj.getChargingTimeExtensionReason())));
        if (itcastDataObj.getCurrentBatteryStartNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("currentBatteryStartNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getCurrentBatteryStartNum())));
        if (!itcastDataObj.getBatteryVoltage().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("batteryVoltage"), Bytes.toBytes(itcastDataObj.getBatteryVoltage()));
        if (itcastDataObj.getChargeSystemVoltage() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("chargeSystemVoltage"), Bytes.toBytes(String.valueOf(itcastDataObj.getChargeSystemVoltage())));
        if (itcastDataObj.getCurrentBatteryCount() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("currentBatteryCount"), Bytes.toBytes(String.valueOf(itcastDataObj.getCurrentBatteryCount())));
        if (itcastDataObj.getBatteryCount() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("batteryCount"), Bytes.toBytes(String.valueOf(itcastDataObj.getBatteryCount())));
        if (itcastDataObj.getChildSystemNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("childSystemNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getChildSystemNum())));
        if (itcastDataObj.getChargeSystemCurrent() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("chargeSystemCurrent"), Bytes.toBytes(String.valueOf(itcastDataObj.getChargeSystemCurrent())));
        if (!itcastDataObj.getGpsTime().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("gpsTime"), Bytes.toBytes(itcastDataObj.getGpsTime()));
        if (itcastDataObj.getEngineFaultCount() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("engineFaultCount"), Bytes.toBytes(String.valueOf(itcastDataObj.getEngineFaultCount())));
        if (!itcastDataObj.getCarId().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("carId"), Bytes.toBytes(itcastDataObj.getCarId()));
        if (itcastDataObj.getCurrentElectricity() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("currentElectricity"), Bytes.toBytes(String.valueOf(itcastDataObj.getCurrentElectricity())));
        if (itcastDataObj.getSingleBatteryUnderVoltageAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("singleBatteryUnderVoltageAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getSingleBatteryUnderVoltageAlarm())));
        if (itcastDataObj.getMaxVoltageBatterySubSystemNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxVoltageBatterySubSystemNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getMaxVoltageBatterySubSystemNum())));
        if (itcastDataObj.getMinTemperatureProbe() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minTemperatureProbe"), Bytes.toBytes(String.valueOf(itcastDataObj.getMinTemperatureProbe())));
        if (itcastDataObj.getDriveMotorNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driveMotorNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriveMotorNum())));
        if (itcastDataObj.getTotalVoltage() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("totalVoltage"), Bytes.toBytes(String.valueOf(itcastDataObj.getTotalVoltage())));
        if (itcastDataObj.getTemperatureDifferenceAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("temperatureDifferenceAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getTemperatureDifferenceAlarm())));
        if (itcastDataObj.getMaxAlarmLevel() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxAlarmLevel"), Bytes.toBytes(String.valueOf(itcastDataObj.getMaxAlarmLevel())));
        if (itcastDataObj.getStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("status"), Bytes.toBytes(String.valueOf(itcastDataObj.getStatus())));
        if (itcastDataObj.getGeerPosition() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("geerPosition"), Bytes.toBytes(String.valueOf(itcastDataObj.getGeerPosition())));
        if (itcastDataObj.getAverageEnergyConsumption() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("averageEnergyConsumption"), Bytes.toBytes(String.valueOf(itcastDataObj.getAverageEnergyConsumption())));
        if (itcastDataObj.getMinVoltageBattery() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minVoltageBattery"), Bytes.toBytes(String.valueOf(itcastDataObj.getMinVoltageBattery())));
        if (itcastDataObj.getGeerStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("geerStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getGeerStatus())));
        if (itcastDataObj.getControllerInputVoltage() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("controllerInputVoltage"), Bytes.toBytes(String.valueOf(itcastDataObj.getControllerInputVoltage())));
        if (itcastDataObj.getControllerTemperature() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("controllerTemperature"), Bytes.toBytes(String.valueOf(itcastDataObj.getControllerTemperature())));
        if (itcastDataObj.getRevolutionSpeed() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("revolutionSpeed"), Bytes.toBytes(String.valueOf(itcastDataObj.getRevolutionSpeed())));
        if (itcastDataObj.getNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("num"), Bytes.toBytes(String.valueOf(itcastDataObj.getNum())));
        if (itcastDataObj.getControllerDcBusCurrent() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("controllerDcBusCurrent"), Bytes.toBytes(String.valueOf(itcastDataObj.getControllerDcBusCurrent())));
        if (itcastDataObj.getTemperature() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("temperature"), Bytes.toBytes(String.valueOf(itcastDataObj.getTemperature())));
        if (itcastDataObj.getTorque() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("torque"), Bytes.toBytes(String.valueOf(itcastDataObj.getTorque())));
        if (itcastDataObj.getState() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("state"), Bytes.toBytes(String.valueOf(itcastDataObj.getState())));
        if (itcastDataObj.getMinVoltageBatteryNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minVoltageBatteryNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getMinVoltageBatteryNum())));
        if (!itcastDataObj.getValidGps().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("validGps"), Bytes.toBytes(itcastDataObj.getValidGps()));
        if (!itcastDataObj.getEngineFaultCodes().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("engineFaultCodes"), Bytes.toBytes(itcastDataObj.getEngineFaultCodes()));
        if (itcastDataObj.getMinTemperatureValue() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minTemperatureValue"), Bytes.toBytes(String.valueOf(itcastDataObj.getMinTemperatureValue())));
        if (itcastDataObj.getChargeStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("chargeStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getChargeStatus())));
        if (!itcastDataObj.getIgnitionTime().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("ignitionTime"), Bytes.toBytes(itcastDataObj.getIgnitionTime()));
        if (itcastDataObj.getTotalOdometer() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("totalOdometer"), Bytes.toBytes(String.valueOf(itcastDataObj.getTotalOdometer())));
        if (itcastDataObj.getAlti() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("alti"), Bytes.toBytes(String.valueOf(itcastDataObj.getAlti())));
        if (itcastDataObj.getSpeed() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("speed"), Bytes.toBytes(String.valueOf(itcastDataObj.getSpeed())));
        if (itcastDataObj.getSocHighAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("socHighAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getSocHighAlarm())));
        if (itcastDataObj.getVehicleStorageDeviceUndervoltageAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vehicleStorageDeviceUndervoltageAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getVehicleStorageDeviceUndervoltageAlarm())));
        if (itcastDataObj.getTotalCurrent() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("totalCurrent"), Bytes.toBytes(String.valueOf(itcastDataObj.getTotalCurrent())));
        if (itcastDataObj.getBatteryAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("batteryAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getBatteryAlarm())));
        if (itcastDataObj.getRechargeableStorageDeviceMismatchAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("rechargeableStorageDeviceMismatchAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getRechargeableStorageDeviceMismatchAlarm())));
        if (itcastDataObj.getIsHistoryPoi() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("isHistoryPoi"), Bytes.toBytes(String.valueOf(itcastDataObj.getIsHistoryPoi())));
        if (itcastDataObj.getVehiclePureDeviceTypeOvercharge() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vehiclePureDeviceTypeOvercharge"), Bytes.toBytes(String.valueOf(itcastDataObj.getVehiclePureDeviceTypeOvercharge())));
        if (itcastDataObj.getMaxVoltageBattery() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxVoltageBattery"), Bytes.toBytes(String.valueOf(itcastDataObj.getMaxVoltageBattery())));
        if (itcastDataObj.getDcdcTemperatureAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dcdcTemperatureAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getDcdcTemperatureAlarm())));
        if (!itcastDataObj.getIsValidGps().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("isValidGps"), Bytes.toBytes(itcastDataObj.getIsValidGps()));
        if (!itcastDataObj.getLastUpdatedTime().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lastUpdatedTime"), Bytes.toBytes(itcastDataObj.getLastUpdatedTime()));
        if (itcastDataObj.getDriveMotorControllerTemperatureAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driveMotorControllerTemperatureAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriveMotorControllerTemperatureAlarm())));
        if (!itcastDataObj.getProbeTemperatures().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("probeTemperatures"), Bytes.toBytes(itcastDataObj.getProbeTemperatures()));
        if (itcastDataObj.getChargeTemperatureProbeNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("chargeTemperatureProbeNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getChargeTemperatureProbeNum())));
        if (itcastDataObj.getIgniteCumulativeMileage() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("igniteCumulativeMileage"), Bytes.toBytes(String.valueOf(itcastDataObj.getIgniteCumulativeMileage())));
        if (itcastDataObj.getDcStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dcStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getDcStatus())));
        if (!itcastDataObj.getRepay().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("repay"), Bytes.toBytes(itcastDataObj.getRepay()));
        if (itcastDataObj.getMaxTemperatureSubSystemNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxTemperatureSubSystemNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getMaxTemperatureSubSystemNum())));
        if (itcastDataObj.getCarStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("carStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getCarStatus())));
        if (itcastDataObj.getMinVoltageBatterySubSystemNum() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minVoltageBatterySubSystemNum"), Bytes.toBytes(String.valueOf(itcastDataObj.getMinVoltageBatterySubSystemNum())));
        if (itcastDataObj.getHeading() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("heading"), Bytes.toBytes(String.valueOf(itcastDataObj.getHeading())));
        if (itcastDataObj.getDriveMotorFaultCount() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driveMotorFaultCount"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriveMotorFaultCount())));
        if (!itcastDataObj.getTuid().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tuid"), Bytes.toBytes(itcastDataObj.getTuid()));
        if (itcastDataObj.getEnergyRecoveryStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("energyRecoveryStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getEnergyRecoveryStatus())));
        if (itcastDataObj.getFireStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("fireStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getFireStatus())));
        if (!itcastDataObj.getTargetType().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("targetType"), Bytes.toBytes(itcastDataObj.getTargetType()));
        if (itcastDataObj.getMaxTemperatureProbe() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxTemperatureProbe"), Bytes.toBytes(String.valueOf(itcastDataObj.getMaxTemperatureProbe())));
        if (!itcastDataObj.getRechargeableStorageDevicesFaultCodes().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("rechargeableStorageDevicesFaultCodes"), Bytes.toBytes(itcastDataObj.getRechargeableStorageDevicesFaultCodes()));
        if (itcastDataObj.getCarMode() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("carMode"), Bytes.toBytes(String.valueOf(itcastDataObj.getCarMode())));
        if (itcastDataObj.getHighVoltageInterlockStateAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("highVoltageInterlockStateAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getHighVoltageInterlockStateAlarm())));
        if (itcastDataObj.getInsulationAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("insulationAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getInsulationAlarm())));
        if (itcastDataObj.getMileageInformation() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("mileageInformation"), Bytes.toBytes(String.valueOf(itcastDataObj.getMileageInformation())));
        if (itcastDataObj.getMaxTemperatureValue() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxTemperatureValue"), Bytes.toBytes(String.valueOf(itcastDataObj.getMaxTemperatureValue())));
        if (itcastDataObj.getOtherFaultCodes().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("otherFaultCodes"), Bytes.toBytes(itcastDataObj.getOtherFaultCodes()));
        if (itcastDataObj.getRemainPower() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("remainPower"), Bytes.toBytes(String.valueOf(itcastDataObj.getRemainPower())));
        if (itcastDataObj.getInsulateResistance() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("insulateResistance"), Bytes.toBytes(String.valueOf(itcastDataObj.getInsulateResistance())));
        if (itcastDataObj.getBatteryLowTemperatureHeater() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("batteryLowTemperatureHeater"), Bytes.toBytes(String.valueOf(itcastDataObj.getBatteryLowTemperatureHeater())));
        if (!itcastDataObj.getFuelConsumption100km().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("fuelConsumption100km"), Bytes.toBytes(itcastDataObj.getFuelConsumption100km()));
        if (!itcastDataObj.getFuelConsumption().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("fuelConsumption"), Bytes.toBytes(itcastDataObj.getFuelConsumption()));
        if (!itcastDataObj.getEngineSpeed().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("engineSpeed"), Bytes.toBytes(itcastDataObj.getEngineSpeed()));
        if (!itcastDataObj.getEngineStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("engineStatus"), Bytes.toBytes(itcastDataObj.getEngineStatus()));
        if (itcastDataObj.getTrunk() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("trunk"), Bytes.toBytes(String.valueOf(itcastDataObj.getTrunk())));
        if (itcastDataObj.getLowBeam() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lowBeam"), Bytes.toBytes(String.valueOf(itcastDataObj.getLowBeam())));
        if (!itcastDataObj.getTriggerLatchOverheatProtect().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("triggerLatchOverheatProtect"), Bytes.toBytes(itcastDataObj.getTriggerLatchOverheatProtect()));
        if (itcastDataObj.getTurnLndicatorRight() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("turnLndicatorRight"), Bytes.toBytes(String.valueOf(itcastDataObj.getTurnLndicatorRight())));
        if (itcastDataObj.getHighBeam() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("highBeam"), Bytes.toBytes(String.valueOf(itcastDataObj.getHighBeam())));
        if (itcastDataObj.getTurnLndicatorLeft() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("turnLndicatorLeft"), Bytes.toBytes(String.valueOf(itcastDataObj.getTurnLndicatorLeft())));
        if (itcastDataObj.getBcuSwVers() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("bcuSwVers"), Bytes.toBytes(String.valueOf(itcastDataObj.getBcuSwVers())));
        if (itcastDataObj.getBcuHwVers() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("bcuHwVers"), Bytes.toBytes(String.valueOf(itcastDataObj.getBcuHwVers())));
        if (itcastDataObj.getBcuOperMod() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("bcuOperMod"), Bytes.toBytes(String.valueOf(itcastDataObj.getBcuOperMod())));
        if (itcastDataObj.getChrgEndReason() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("chrgEndReason"), Bytes.toBytes(String.valueOf(itcastDataObj.getChrgEndReason())));
        if (!itcastDataObj.getBCURegenEngDisp().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("BCURegenEngDisp"), Bytes.toBytes(itcastDataObj.getBCURegenEngDisp()));
        if (itcastDataObj.getBCURegenCpDisp() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("BCURegenCpDisp"), Bytes.toBytes(String.valueOf(itcastDataObj.getBCURegenCpDisp())));
        if (itcastDataObj.getBcuChrgMod() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("bcuChrgMod"), Bytes.toBytes(String.valueOf(itcastDataObj.getBcuChrgMod())));
        if (itcastDataObj.getBatteryChargeStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("batteryChargeStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getBatteryChargeStatus())));
        if (!itcastDataObj.getBcuFaultCodes().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("BcuFaultCodes"), Bytes.toBytes(itcastDataObj.getBcuFaultCodes()));
        if (itcastDataObj.getBcuFltRnk() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("bcuFltRnk"), Bytes.toBytes(String.valueOf(itcastDataObj.getBcuFltRnk())));
        if (!itcastDataObj.getBattPoleTOver().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("battPoleTOver"), Bytes.toBytes(itcastDataObj.getBattPoleTOver()));
        if (itcastDataObj.getBcuSOH() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("bcuSOH"), Bytes.toBytes(String.valueOf(itcastDataObj.getBcuSOH())));
        if (itcastDataObj.getBattIntrHeatActive() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("battIntrHeatActive"), Bytes.toBytes(String.valueOf(itcastDataObj.getBattIntrHeatActive())));
        if (itcastDataObj.getBattIntrHeatReq() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("battIntrHeatReq"), Bytes.toBytes(String.valueOf(itcastDataObj.getBattIntrHeatReq())));
        if (!itcastDataObj.getBCUBattTarT().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("BCUBattTarT"), Bytes.toBytes(itcastDataObj.getBCUBattTarT()));
        if (itcastDataObj.getBattExtHeatReq() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("battExtHeatReq"), Bytes.toBytes(String.valueOf(itcastDataObj.getBattExtHeatReq())));
        if (!itcastDataObj.getBCUMaxChrgPwrLongT().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("BCUMaxChrgPwrLongT"), Bytes.toBytes(itcastDataObj.getBCUMaxChrgPwrLongT()));
        if (!itcastDataObj.getBCUMaxDchaPwrLongT().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("BCUMaxDchaPwrLongT"), Bytes.toBytes(itcastDataObj.getBCUMaxDchaPwrLongT()));
        if (!itcastDataObj.getBCUTotalRegenEngDisp().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("BCUTotalRegenEngDisp"), Bytes.toBytes(itcastDataObj.getBCUTotalRegenEngDisp()));
        if (!itcastDataObj.getBCUTotalRegenCpDisp().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("BCUTotalRegenCpDisp"), Bytes.toBytes(itcastDataObj.getBCUTotalRegenCpDisp()));
        if (itcastDataObj.getDcdcFltRnk() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dcdcFltRnk"), Bytes.toBytes(String.valueOf(itcastDataObj.getDcdcFltRnk())));
        if (!itcastDataObj.getDcdcFaultCode().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("DcdcFaultCode"), Bytes.toBytes(itcastDataObj.getDcdcFaultCode()));
        if (itcastDataObj.getDcdcOutpCrrt() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dcdcOutpCrrt"), Bytes.toBytes(String.valueOf(itcastDataObj.getDcdcOutpCrrt())));
        if (itcastDataObj.getDcdcOutpU() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dcdcOutpU"), Bytes.toBytes(String.valueOf(itcastDataObj.getDcdcOutpU())));
        if (itcastDataObj.getDcdcAvlOutpPwr() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dcdcAvlOutpPwr"), Bytes.toBytes(String.valueOf(itcastDataObj.getDcdcAvlOutpPwr())));
        if (!itcastDataObj.getAbsActiveStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("absActiveStatus"), Bytes.toBytes(itcastDataObj.getAbsActiveStatus()));
        if (!itcastDataObj.getAbsStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("absStatus"), Bytes.toBytes(itcastDataObj.getAbsStatus()));
        if (!itcastDataObj.getVcuBrkErr().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("VcuBrkErr"), Bytes.toBytes(itcastDataObj.getVcuBrkErr()));
        if (!itcastDataObj.getEPB_AchievedClampForce().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("EPB_AchievedClampForce"), Bytes.toBytes(itcastDataObj.getEPB_AchievedClampForce()));
        if (!itcastDataObj.getEpbSwitchPosition().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("epbSwitchPosition"), Bytes.toBytes(itcastDataObj.getEpbSwitchPosition()));
        if (!itcastDataObj.getEpbStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("epbStatus"), Bytes.toBytes(itcastDataObj.getEpbStatus()));
        if (!itcastDataObj.getEspActiveStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("espActiveStatus"), Bytes.toBytes(itcastDataObj.getEspActiveStatus()));
        if (!itcastDataObj.getEspFunctionStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("espFunctionStatus"), Bytes.toBytes(itcastDataObj.getEspFunctionStatus()));
        if (!itcastDataObj.getESP_TCSFailStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("ESP_TCSFailStatus"), Bytes.toBytes(itcastDataObj.getESP_TCSFailStatus()));
        if (!itcastDataObj.getHhcActive().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("hhcActive"), Bytes.toBytes(itcastDataObj.getHhcActive()));
        if (!itcastDataObj.getTcsActive().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tcsActive"), Bytes.toBytes(itcastDataObj.getTcsActive()));
        if (!itcastDataObj.getEspMasterCylinderBrakePressure().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("espMasterCylinderBrakePressure"), Bytes.toBytes(itcastDataObj.getEspMasterCylinderBrakePressure()));
        if (!itcastDataObj.getESP_MasterCylinderBrakePressureValid().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("ESP_MasterCylinderBrakePressureValid"), Bytes.toBytes(itcastDataObj.getESP_MasterCylinderBrakePressureValid()));
        if (!itcastDataObj.getEspTorqSensorStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("espTorqSensorStatus"), Bytes.toBytes(itcastDataObj.getEspTorqSensorStatus()));
        if (!itcastDataObj.getEPS_EPSFailed().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("EPS_EPSFailed"), Bytes.toBytes(itcastDataObj.getEPS_EPSFailed()));
        if (!itcastDataObj.getSasFailure().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sasFailure"), Bytes.toBytes(itcastDataObj.getSasFailure()));
        if (!itcastDataObj.getSasSteeringAngleSpeed().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sasSteeringAngleSpeed"), Bytes.toBytes(itcastDataObj.getSasSteeringAngleSpeed()));
        if (!itcastDataObj.getSasSteeringAngle().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sasSteeringAngle"), Bytes.toBytes(itcastDataObj.getSasSteeringAngle()));
        if (!itcastDataObj.getSasSteeringAngleValid().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sasSteeringAngleValid"), Bytes.toBytes(itcastDataObj.getSasSteeringAngleValid()));
        if (!itcastDataObj.getEspSteeringTorque().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("espSteeringTorque"), Bytes.toBytes(itcastDataObj.getEspSteeringTorque()));
        if (itcastDataObj.getAcReq() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("acReq"), Bytes.toBytes(String.valueOf(itcastDataObj.getAcReq())));
        if (itcastDataObj.getAcSystemFailure() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("acSystemFailure"), Bytes.toBytes(String.valueOf(itcastDataObj.getAcSystemFailure())));
        if (itcastDataObj.getPtcPwrAct() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("ptcPwrAct"), Bytes.toBytes(String.valueOf(itcastDataObj.getPtcPwrAct())));
        if (itcastDataObj.getPlasmaStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("plasmaStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getPlasmaStatus())));
        if (itcastDataObj.getBattInTemperature() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("battInTemperature"), Bytes.toBytes(String.valueOf(itcastDataObj.getBattInTemperature())));
        if (!itcastDataObj.getBattWarmLoopSts().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("battWarmLoopSts"), Bytes.toBytes(itcastDataObj.getBattWarmLoopSts()));
        if (!itcastDataObj.getBattCoolngLoopSts().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("battCoolngLoopSts"), Bytes.toBytes(itcastDataObj.getBattCoolngLoopSts()));
        if (!itcastDataObj.getBattCoolActv().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("battCoolActv"), Bytes.toBytes(itcastDataObj.getBattCoolActv()));
        if (itcastDataObj.getMotorOutTemperature() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("motorOutTemperature"), Bytes.toBytes(String.valueOf(itcastDataObj.getMotorOutTemperature())));
        if (!itcastDataObj.getPowerStatusFeedBack().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("powerStatusFeedBack"), Bytes.toBytes(String.valueOf(itcastDataObj.getPowerStatusFeedBack())));
        if (itcastDataObj.getAC_RearDefrosterSwitch() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("AC_RearDefrosterSwitch"), Bytes.toBytes(String.valueOf(itcastDataObj.getAC_RearDefrosterSwitch())));
        if (itcastDataObj.getRearFoglamp() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("rearFoglamp"), Bytes.toBytes(String.valueOf(itcastDataObj.getRearFoglamp())));
        if (itcastDataObj.getDriverDoorLock() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driverDoorLock"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriverDoorLock())));
        if (itcastDataObj.getAcDriverReqTemp() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("acDriverReqTemp"), Bytes.toBytes(String.valueOf(itcastDataObj.getAcDriverReqTemp())));
        if (itcastDataObj.getKeyAlarm() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("keyAlarm"), Bytes.toBytes(String.valueOf(itcastDataObj.getKeyAlarm())));
        if (itcastDataObj.getAirCleanStsRemind() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("airCleanStsRemind"), Bytes.toBytes(String.valueOf(itcastDataObj.getAirCleanStsRemind())));
        if (itcastDataObj.getRecycleType() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("recycleType"), Bytes.toBytes(String.valueOf(itcastDataObj.getRecycleType())));
        if (!itcastDataObj.getStartControlsignal().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("startControlsignal"), Bytes.toBytes(itcastDataObj.getStartControlsignal()));
        if (itcastDataObj.getAirBagWarningLamp() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("airBagWarningLamp"), Bytes.toBytes(String.valueOf(itcastDataObj.getAirBagWarningLamp())));
        if (itcastDataObj.getFrontDefrosterSwitch() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frontDefrosterSwitch"), Bytes.toBytes(String.valueOf(itcastDataObj.getFrontDefrosterSwitch())));
        if (!itcastDataObj.getFrontBlowType().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frontBlowType"), Bytes.toBytes(itcastDataObj.getFrontBlowType()));
        if (itcastDataObj.getFrontReqWindLevel() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frontReqWindLevel"), Bytes.toBytes(String.valueOf(itcastDataObj.getFrontReqWindLevel())));
        if (!itcastDataObj.getBcmFrontWiperStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("bcmFrontWiperStatus"), Bytes.toBytes(itcastDataObj.getBcmFrontWiperStatus()));
        if (!itcastDataObj.getTmsPwrAct().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tmsPwrAct"), Bytes.toBytes(itcastDataObj.getTmsPwrAct()));
        if (itcastDataObj.getKeyUndetectedAlarmSign() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("keyUndetectedAlarmSign"), Bytes.toBytes(String.valueOf(itcastDataObj.getKeyUndetectedAlarmSign())));
        if (!itcastDataObj.getPositionLamp().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("positionLamp"), Bytes.toBytes(itcastDataObj.getPositionLamp()));
        if (itcastDataObj.getDriverReqTempModel() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driverReqTempModel"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriverReqTempModel())));
        if (itcastDataObj.getTurnLightSwitchSts() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("turnLightSwitchSts"), Bytes.toBytes(String.valueOf(itcastDataObj.getTurnLightSwitchSts())));
        if (itcastDataObj.getAutoHeadlightStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("autoHeadlightStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getAutoHeadlightStatus())));
        if (itcastDataObj.getDriverDoor() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driverDoor"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriverDoor())));
        if (!itcastDataObj.getIpuFaultCodes().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("IpuFaultCodes"), Bytes.toBytes(itcastDataObj.getIpuFaultCodes()));
        if (itcastDataObj.getFrntIpuFltRnk() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frntIpuFltRnk"), Bytes.toBytes(String.valueOf(itcastDataObj.getFrntIpuFltRnk())));
        if (!itcastDataObj.getFrontIpuSwVers().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frontIpuSwVers"), Bytes.toBytes(itcastDataObj.getFrontIpuSwVers()));
        if (itcastDataObj.getFrontIpuHwVers() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frontIpuHwVers"), Bytes.toBytes(String.valueOf(itcastDataObj.getFrontIpuHwVers())));
        if (itcastDataObj.getFrntMotTqLongTermMax() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frntMotTqLongTermMax"), Bytes.toBytes(String.valueOf(itcastDataObj.getFrntMotTqLongTermMax())));
        if (itcastDataObj.getFrntMotTqLongTermMin() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("frntMotTqLongTermMin"), Bytes.toBytes(String.valueOf(itcastDataObj.getFrntMotTqLongTermMin())));
        if (itcastDataObj.getCpvValue() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("cpvValue"), Bytes.toBytes(String.valueOf(itcastDataObj.getCpvValue())));
        if (itcastDataObj.getObcChrgSts() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("obcChrgSts"), Bytes.toBytes(String.valueOf(itcastDataObj.getObcChrgSts())));
        if (!itcastDataObj.getObcFltRnk().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("obcFltRnk"), Bytes.toBytes(itcastDataObj.getObcFltRnk()));
        if (itcastDataObj.getObcChrgInpAcI() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("obcChrgInpAcI"), Bytes.toBytes(String.valueOf(itcastDataObj.getObcChrgInpAcI())));
        if (itcastDataObj.getObcChrgInpAcU() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("obcChrgInpAcU"), Bytes.toBytes(String.valueOf(itcastDataObj.getObcChrgInpAcU())));
        if (itcastDataObj.getObcChrgDcI() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("obcChrgDcI"), Bytes.toBytes(String.valueOf(itcastDataObj.getObcChrgDcI())));
        if (itcastDataObj.getObcChrgDcU() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("obcChrgDcU"), Bytes.toBytes(String.valueOf(itcastDataObj.getObcChrgDcU())));
        if (itcastDataObj.getObcTemperature() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("obcTemperature"), Bytes.toBytes(String.valueOf(itcastDataObj.getObcTemperature())));
        if (itcastDataObj.getObcMaxChrgOutpPwrAvl() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("obcMaxChrgOutpPwrAvl"), Bytes.toBytes(String.valueOf(itcastDataObj.getObcMaxChrgOutpPwrAvl())));
        if (itcastDataObj.getPassengerBuckleSwitch() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("passengerBuckleSwitch"), Bytes.toBytes(String.valueOf(itcastDataObj.getPassengerBuckleSwitch())));
        if (!itcastDataObj.getCrashlfo().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("crashlfo"), Bytes.toBytes(itcastDataObj.getCrashlfo()));
        if (itcastDataObj.getDriverBuckleSwitch() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driverBuckleSwitch"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriverBuckleSwitch())));
        if (!itcastDataObj.getEngineStartHibit().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("engineStartHibit"), Bytes.toBytes(itcastDataObj.getEngineStartHibit()));
        if (!itcastDataObj.getLockCommand().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lockCommand"), Bytes.toBytes(itcastDataObj.getLockCommand()));
        if (!itcastDataObj.getSearchCarReq().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("searchCarReq"), Bytes.toBytes(itcastDataObj.getSearchCarReq()));
        if (!itcastDataObj.getAcTempValueReq().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("acTempValueReq"), Bytes.toBytes(itcastDataObj.getAcTempValueReq()));
        if (!itcastDataObj.getVcuFaultCode().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("VcuFaultCode"), Bytes.toBytes(itcastDataObj.getVcuFaultCode()));
        if (!itcastDataObj.getVcuErrAmnt().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vcuErrAmnt"), Bytes.toBytes(itcastDataObj.getVcuErrAmnt()));
        if (itcastDataObj.getVcuSwVers() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vcuSwVers"), Bytes.toBytes(String.valueOf(itcastDataObj.getVcuSwVers())));
        if (itcastDataObj.getVcuHwVers() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vcuHwVers"), Bytes.toBytes(String.valueOf(itcastDataObj.getVcuHwVers())));
        if (!itcastDataObj.getLowSpdWarnStatus().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lowSpdWarnStatus"), Bytes.toBytes(itcastDataObj.getLowSpdWarnStatus()));
        if (itcastDataObj.getLowBattChrgRqe() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lowBattChrgRqe"), Bytes.toBytes(String.valueOf(itcastDataObj.getLowBattChrgRqe())));
        if (!itcastDataObj.getLowBattChrgSts().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lowBattChrgSts"), Bytes.toBytes(itcastDataObj.getLowBattChrgSts()));
        if (itcastDataObj.getLowBattU() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lowBattU"), Bytes.toBytes(String.valueOf(itcastDataObj.getLowBattU())));
        if (itcastDataObj.getHandlebrakeStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("handlebrakeStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getHandlebrakeStatus())));
        if (!itcastDataObj.getShiftPositionValid().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("shiftPositionValid"), Bytes.toBytes(itcastDataObj.getShiftPositionValid()));
        if (!itcastDataObj.getAccPedalValid().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("accPedalValid"), Bytes.toBytes(itcastDataObj.getAccPedalValid()));
        if (itcastDataObj.getDriveMode() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driveMode"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriveMode())));
        if (itcastDataObj.getDriveModeButtonStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("driveModeButtonStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getDriveModeButtonStatus())));
        if (itcastDataObj.getVCUSRSCrashOutpSts() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("VCUSRSCrashOutpSts"), Bytes.toBytes(String.valueOf(itcastDataObj.getVCUSRSCrashOutpSts())));
        if (itcastDataObj.getTextDispEna() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("textDispEna"), Bytes.toBytes(String.valueOf(itcastDataObj.getTextDispEna())));
        if (itcastDataObj.getCrsCtrlStatus() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("crsCtrlStatus"), Bytes.toBytes(String.valueOf(itcastDataObj.getCrsCtrlStatus())));
        if (itcastDataObj.getCrsTarSpd() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("crsTarSpd"), Bytes.toBytes(String.valueOf(itcastDataObj.getCrsTarSpd())));
        if (itcastDataObj.getCrsTextDisp() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("crsTextDisp"), Bytes.toBytes(String.valueOf(itcastDataObj.getCrsTextDisp())));
        if (itcastDataObj.getKeyOn() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("keyOn"), Bytes.toBytes(String.valueOf(itcastDataObj.getKeyOn())));
        if (itcastDataObj.getVehPwrlim() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vehPwrlim"), Bytes.toBytes(String.valueOf(itcastDataObj.getVehPwrlim())));
        if (!itcastDataObj.getVehCfgInfo().isEmpty()) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vehCfgInfo"), Bytes.toBytes(itcastDataObj.getVehCfgInfo()));
        if (itcastDataObj.getVacBrkPRmu() != -999999) put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vacBrkPRmu"), Bytes.toBytes(itcastDataObj.getVacBrkPRmu()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("processTime"), Bytes.toBytes(DateUtil.getCurrentDateTime()));
        return put;
    }

}
