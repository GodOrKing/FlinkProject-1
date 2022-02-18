package cn.itcast.utils;

import cn.itcast.entity.ItcastDataObj;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * 将消费到的kafka的json字符串转换成ItcastDataObj对象
 */
public class JsonParseUtil {
    //使用指定类（JsonParseUtil），初始化日志对象，方便在日志输出的时候，打印出来日志信息所属的类
    private final static Logger logger = LoggerFactory.getLogger(JsonParseUtil.class);

    /**
     * 传递json字符串，返回解析后的javaBean对象
     * {"gearDriveForce":0,"batteryConsistencyDifferenceAlarm":0,"soc":94,"socJumpAlarm":0,"satNum":10,"caterpillaringFunction":0,"socLowAlarm":0,"chargingGunConnectionState":0,"minTemperatureSubSystemNum":1,"chargedElectronicLockStatus":0,"terminalTime":"2019-11-20 15:33:41","maxVoltageBatteryNum":49,"singleBatteryOverVoltageAlarm":0,"otherFaultCount":0,"vehicleStorageDeviceOvervoltageAlarm":0,"brakeSystemAlarm":0,"serverTime":"2019-11-20 15:34:00.154","vin":"LS6A2E0E3KA006811","rechargeableStorageDevicesFaultCount":0,"driveMotorTemperatureAlarm":0,"remainedPowerMile":0,"dcdcStatusAlarm":0,"gearBrakeForce":1,"lat":29.0646,"driveMotorFaultCodes":"","vehicleSpeed":0.0,"lng":119.50206,"gpsTime":"2019-11-20 15:33:41","nevChargeSystemVoltageDtoList":[{"currentBatteryStartNum":1,"batteryVoltage":[4.15,4.15,4.147,4.148,4.149,4.148,4.139,4.147,4.146,4.152,4.149,4.149,4.15,4.149,4.146,4.148,4.152,4.147,4.149,4.15,4.146,4.15,4.149,4.148,4.148,4.149,4.148,4.149,4.152,4.141,4.146,4.151,4.152,4.154,4.15,4.147,4.15,4.144,4.146,4.145,4.149,4.148,4.147,4.148,4.144,4.143,4.147,4.141,4.156,4.155,4.15,4.15,4.151,4.156,4.153,4.145,4.151,4.144,4.15,4.152,4.145,4.15,4.148,4.149,4.151,4.156,4.152,4.152,4.151,4.142,4.149,4.151,4.148,4.145,4.148,4.146,4.148,4.146,4.151,4.138,4.147,4.138,4.146,4.142,4.149,4.15,4.146,4.148,4.143,4.146,4.147,4.147,4.155,4.151,4.141,4.147],"chargeSystemVoltage":398.2,"currentBatteryCount":96,"batteryCount":96,"childSystemNum":1,"chargeSystemCurrent":0.2999878}],"engineFaultCount":0,"currentElectricity":94,"singleBatteryUnderVoltageAlarm":0,"maxVoltageBatterySubSystemNum":1,"minTemperatureProbe":13,"driveMotorNum":1,"totalVoltage":398.2,"maxAlarmLevel":0,"temperatureDifferenceAlarm":0,"averageEnergyConsumption":0.0,"minVoltageBattery":4.138,"driveMotorData":[{"controllerInputVoltage":399.0,"controllerTemperature":38,"revolutionSpeed":0,"num":1,"controllerDcBusCurrent":0.0,"length":0,"temperature":40,"torque":0.0,"state":4,"type":0,"MAX_BYTE_VALUE":127}],"shiftPositionStatus":0,"minVoltageBatteryNum":80,"engineFaultCodes":"","minTemperatureValue":17,"chargeStatus":3,"deviceTime":"2019-11-20 15:33:41","shiftPosition":0,"totalOdometer":38595.0,"alti":57.0,"speed":0.0,"socHighAlarm":0,"vehicleStorageDeviceUndervoltageAlarm":0,"totalCurrent":0.3,"batteryAlarm":0,"rechargeableStorageDeviceMismatchAlarm":0,"isHistoryPoi":0,"maxVoltageBattery":4.156,"vehiclePureDeviceTypeOvercharge":0,"dcdcTemperatureAlarm":0,"isValidGps":true,"lastUpdatedTime":"2019-11-20 15:34:00.154","driveMotorControllerTemperatureAlarm":0,"nevChargeSystemTemperatureDtoList":[{"probeTemperatures":[18,18,18,20,19,19,19,20,19,19,18,19,17,17,17,17,17,17,18,18,18,17,18,18,17,17,17,17,17,17,18,18],"chargeTemperatureProbeNum":32,"childSystemNum":1}],"igniteCumulativeMileage":0.0,"dcStatus":1,"maxTemperatureSubSystemNum":1,"carStatus":2,"minVoltageBatterySubSystemNum":1,"heading":2.68,"driveMotorFaultCount":0,"tuid":"50003001190517140000000518553162","energyRecoveryStatus":0,"targetType":"VehicleRealtimeDto","maxTemperatureProbe":4,"rechargeableStorageDevicesFaultCodes":"","carMode":1,"highVoltageInterlockStateAlarm":0,"insulationAlarm":0,"maxTemperatureValue":20,"otherFaultCodes":"","remainPower":94.00001,"insulateResistance":6417,"batteryLowTemperatureHeater":0}
     * @param jsonStr
     * @return
     */
    public static ItcastDataObj parseJsonToObject(String jsonStr){
        //定义需要返回的javaBean对象
        ItcastDataObj itcastDataObj = new ItcastDataObj();

        try {
            //将json字符串解析为Map对象
            HashMap<String, Object> vehicleMap = jsonStrToMap(jsonStr);
            itcastDataObj.setGearDriveForce(convertIntType("gearDriveForce", vehicleMap));
            itcastDataObj.setBatteryConsistencyDifferenceAlarm(convertIntType("batteryConsistencyDifferenceAlarm", vehicleMap));
            itcastDataObj.setSoc(convertIntType("soc", vehicleMap));
            itcastDataObj.setSocJumpAlarm(convertIntType("socJumpAlarm", vehicleMap));
            itcastDataObj.setCaterpillaringFunction(convertIntType("caterpillaringFunction", vehicleMap));
            itcastDataObj.setSatNum(convertIntType("satNum", vehicleMap));
            itcastDataObj.setSocLowAlarm(convertIntType("socLowAlarm", vehicleMap));
            itcastDataObj.setChargingGunConnectionState(convertIntType("chargingGunConnectionState", vehicleMap));
            itcastDataObj.setMinTemperatureSubSystemNum(convertIntType("minTemperatureSubSystemNum", vehicleMap));
            itcastDataObj.setChargedElectronicLockStatus(convertIntType("chargedElectronicLockStatus", vehicleMap));
            itcastDataObj.setMaxVoltageBatteryNum(convertIntType("maxVoltageBatteryNum", vehicleMap));
            itcastDataObj.setTerminalTime(convertStringType("terminalTime", vehicleMap));
            itcastDataObj.setSingleBatteryOverVoltageAlarm(convertIntType("singleBatteryOverVoltageAlarm", vehicleMap));
            itcastDataObj.setOtherFaultCount(convertIntType("otherFaultCount", vehicleMap));
            itcastDataObj.setVehicleStorageDeviceOvervoltageAlarm(convertIntType("vehicleStorageDeviceOvervoltageAlarm", vehicleMap));
            itcastDataObj.setBrakeSystemAlarm(convertIntType("brakeSystemAlarm", vehicleMap));
            itcastDataObj.setServerTime(convertStringType("serverTime", vehicleMap));
            itcastDataObj.setVin(convertStringType("vin", vehicleMap).toUpperCase());
            itcastDataObj.setRechargeableStorageDevicesFaultCount(convertIntType("rechargeableStorageDevicesFaultCount", vehicleMap));
            itcastDataObj.setDriveMotorTemperatureAlarm(convertIntType("driveMotorTemperatureAlarm", vehicleMap));
            itcastDataObj.setGearBrakeForce(convertIntType("gearBrakeForce", vehicleMap));
            itcastDataObj.setDcdcStatusAlarm(convertIntType("dcdcStatusAlarm", vehicleMap));
            itcastDataObj.setLat(convertDoubleType("lat", vehicleMap));
            itcastDataObj.setDriveMotorFaultCodes(convertStringType("driveMotorFaultCodes", vehicleMap));
            itcastDataObj.setDeviceType(convertStringType("deviceType", vehicleMap));
            itcastDataObj.setVehicleSpeed(convertDoubleType("vehicleSpeed", vehicleMap));
            itcastDataObj.setLng(convertDoubleType("lng", vehicleMap));
            itcastDataObj.setChargingTimeExtensionReason(convertIntType("chargingTimeExtensionReason", vehicleMap));
            itcastDataObj.setGpsTime(convertStringType("gpsTime", vehicleMap));
            itcastDataObj.setEngineFaultCount(convertIntType("engineFaultCount", vehicleMap));
            itcastDataObj.setCarId(convertStringType("carId", vehicleMap));
            itcastDataObj.setCurrentElectricity(convertDoubleType("vehicleSpeed", vehicleMap));
            itcastDataObj.setSingleBatteryUnderVoltageAlarm(convertIntType("singleBatteryUnderVoltageAlarm", vehicleMap));
            itcastDataObj.setMaxVoltageBatterySubSystemNum(convertIntType("maxVoltageBatterySubSystemNum", vehicleMap));
            itcastDataObj.setMinTemperatureProbe(convertIntType("minTemperatureProbe", vehicleMap));
            itcastDataObj.setDriveMotorNum(convertIntType("driveMotorNum", vehicleMap));
            itcastDataObj.setTotalVoltage(convertDoubleType("totalVoltage", vehicleMap));
            itcastDataObj.setTemperatureDifferenceAlarm(convertIntType("temperatureDifferenceAlarm", vehicleMap));
            itcastDataObj.setMaxAlarmLevel(convertIntType("maxAlarmLevel", vehicleMap));
            itcastDataObj.setStatus(convertIntType("status", vehicleMap));
            itcastDataObj.setGeerPosition(convertIntType("geerPosition", vehicleMap));
            itcastDataObj.setAverageEnergyConsumption(convertDoubleType("averageEnergyConsumption", vehicleMap));
            itcastDataObj.setMinVoltageBattery(convertDoubleType("minVoltageBattery", vehicleMap));
            itcastDataObj.setGeerStatus(convertIntType("geerStatus", vehicleMap));
            itcastDataObj.setMinVoltageBatteryNum(convertIntType("minVoltageBatteryNum", vehicleMap));
            itcastDataObj.setValidGps(convertStringType("validGps", vehicleMap));
            itcastDataObj.setEngineFaultCodes(convertStringType("engineFaultCodes", vehicleMap));
            itcastDataObj.setMinTemperatureValue(convertDoubleType("minTemperatureValue", vehicleMap));
            itcastDataObj.setChargeStatus(convertIntType("chargeStatus", vehicleMap));
            itcastDataObj.setIgnitionTime(convertStringType("ignitionTime", vehicleMap));
            itcastDataObj.setTotalOdometer(convertDoubleType("totalOdometer", vehicleMap));
            itcastDataObj.setAlti(convertDoubleType("alti", vehicleMap));
            itcastDataObj.setSpeed(convertDoubleType("speed", vehicleMap));
            itcastDataObj.setSocHighAlarm(convertIntType("socHighAlarm", vehicleMap));
            itcastDataObj.setVehicleStorageDeviceUndervoltageAlarm(convertIntType("vehicleStorageDeviceUndervoltageAlarm", vehicleMap));
            itcastDataObj.setTotalCurrent(convertDoubleType("totalCurrent", vehicleMap));
            itcastDataObj.setBatteryAlarm(convertIntType("batteryAlarm", vehicleMap));
            itcastDataObj.setRechargeableStorageDeviceMismatchAlarm(convertIntType("rechargeableStorageDeviceMismatchAlarm", vehicleMap));
            itcastDataObj.setIsHistoryPoi(convertIntType("isHistoryPoi", vehicleMap));
            itcastDataObj.setVehiclePureDeviceTypeOvercharge(convertIntType("vehiclePureDeviceTypeOvercharge", vehicleMap));
            itcastDataObj.setMaxVoltageBattery(convertDoubleType("maxVoltageBattery", vehicleMap));
            itcastDataObj.setDcdcTemperatureAlarm(convertIntType("dcdcTemperatureAlarm", vehicleMap));
            itcastDataObj.setIsValidGps(convertStringType("isValidGps", vehicleMap));
            itcastDataObj.setLastUpdatedTime(convertStringType("lastUpdatedTime", vehicleMap));
            itcastDataObj.setDriveMotorControllerTemperatureAlarm(convertIntType("driveMotorControllerTemperatureAlarm", vehicleMap));
            itcastDataObj.setIgniteCumulativeMileage(convertDoubleType("igniteCumulativeMileage", vehicleMap));
            itcastDataObj.setDcStatus(convertIntType("dcStatus", vehicleMap));
            itcastDataObj.setRepay(convertStringType("repay", vehicleMap));
            itcastDataObj.setMaxTemperatureSubSystemNum(convertIntType("maxTemperatureSubSystemNum", vehicleMap));
            itcastDataObj.setMinVoltageBatterySubSystemNum(convertIntType("minVoltageBatterySubSystemNum", vehicleMap));
            itcastDataObj.setHeading(convertDoubleType("heading", vehicleMap));
            itcastDataObj.setTuid(convertStringType("tuid", vehicleMap));
            itcastDataObj.setEnergyRecoveryStatus(convertIntType("energyRecoveryStatus", vehicleMap));
            itcastDataObj.setFireStatus(convertIntType("fireStatus", vehicleMap));
            itcastDataObj.setTargetType(convertStringType("targetType", vehicleMap));
            itcastDataObj.setMaxTemperatureProbe(convertIntType("maxTemperatureProbe", vehicleMap));
            itcastDataObj.setRechargeableStorageDevicesFaultCodes(convertStringType("rechargeableStorageDevicesFaultCodes", vehicleMap));
            itcastDataObj.setCarMode(convertIntType("carMode", vehicleMap));
            itcastDataObj.setHighVoltageInterlockStateAlarm(convertIntType("highVoltageInterlockStateAlarm", vehicleMap));
            itcastDataObj.setInsulationAlarm(convertIntType("insulationAlarm", vehicleMap));
            itcastDataObj.setMileageInformation(convertIntType("mileageInformation", vehicleMap));
            itcastDataObj.setMaxTemperatureValue(convertDoubleType("maxTemperatureValue", vehicleMap));
            itcastDataObj.setOtherFaultCodes(convertStringType("otherFaultCodes", vehicleMap));
            itcastDataObj.setRemainPower(convertDoubleType("remainPower", vehicleMap));
            itcastDataObj.setInsulateResistance(convertIntType("insulateResistance", vehicleMap));
            itcastDataObj.setBatteryLowTemperatureHeater(convertIntType("batteryLowTemperatureHeater", vehicleMap));
            itcastDataObj.setFuelConsumption100km(convertStringType("fuelConsumption100km", vehicleMap));
            itcastDataObj.setFuelConsumption(convertStringType("fuelConsumption", vehicleMap));
            itcastDataObj.setEngineSpeed(convertStringType("engineSpeed", vehicleMap));
            itcastDataObj.setEngineStatus(convertStringType("engineStatus", vehicleMap));
            itcastDataObj.setTrunk(convertIntType("trunk", vehicleMap));
            itcastDataObj.setLowBeam(convertIntType("lowBeam", vehicleMap));
            itcastDataObj.setTriggerLatchOverheatProtect(convertStringType("triggerLatchOverheatProtect", vehicleMap));
            itcastDataObj.setTurnLndicatorRight(convertIntType("turnLndicatorRight", vehicleMap));
            itcastDataObj.setHighBeam(convertIntType("highBeam", vehicleMap));
            itcastDataObj.setTurnLndicatorLeft(convertIntType("turnLndicatorLeft", vehicleMap));
            itcastDataObj.setBcuSwVers(convertIntType("bcuSwVers", vehicleMap));
            itcastDataObj.setBcuHwVers(convertIntType("bcuHwVers", vehicleMap));
            itcastDataObj.setBcuOperMod(convertIntType("bcuOperMod", vehicleMap));
            itcastDataObj.setChrgEndReason(convertIntType("chrgEndReason", vehicleMap));
            itcastDataObj.setBCURegenEngDisp(convertStringType("BCURegenEngDisp", vehicleMap));
            itcastDataObj.setBCURegenCpDisp(convertIntType("BCURegenCpDisp", vehicleMap));
            itcastDataObj.setBcuChrgMod(convertIntType("bcuChrgMod", vehicleMap));
            itcastDataObj.setBatteryChargeStatus(convertIntType("batteryChargeStatus", vehicleMap));
            itcastDataObj.setBcuFltRnk(convertIntType("bcuFltRnk", vehicleMap));
            itcastDataObj.setBattPoleTOver(convertStringType("battPoleTOver", vehicleMap));
            itcastDataObj.setBcuSOH(convertDoubleType("bcuSOH", vehicleMap));
            itcastDataObj.setBattIntrHeatActive(convertIntType("battIntrHeatActive", vehicleMap));
            itcastDataObj.setBattIntrHeatReq(convertIntType("battIntrHeatReq", vehicleMap));
            itcastDataObj.setBCUBattTarT(convertStringType("BCUBattTarT", vehicleMap));
            itcastDataObj.setBattExtHeatReq(convertIntType("battExtHeatReq", vehicleMap));
            itcastDataObj.setBCUMaxChrgPwrLongT(convertStringType("BCUMaxChrgPwrLongT", vehicleMap));
            itcastDataObj.setBCUMaxDchaPwrLongT(convertStringType("BCUMaxDchaPwrLongT", vehicleMap));
            itcastDataObj.setBCUTotalRegenEngDisp(convertStringType("BCUTotalRegenEngDisp", vehicleMap));
            itcastDataObj.setBCUTotalRegenCpDisp(convertStringType("BCUTotalRegenCpDisp ", vehicleMap));
            itcastDataObj.setDcdcFltRnk(convertIntType("dcdcFltRnk", vehicleMap));
            itcastDataObj.setDcdcOutpCrrt(convertDoubleType("dcdcOutpCrrt", vehicleMap));
            itcastDataObj.setDcdcOutpU(convertDoubleType("dcdcOutpU", vehicleMap));
            itcastDataObj.setDcdcAvlOutpPwr(convertIntType("dcdcAvlOutpPwr", vehicleMap));
            itcastDataObj.setAbsActiveStatus(convertStringType("absActiveStatus", vehicleMap));
            itcastDataObj.setAbsStatus(convertStringType("absStatus", vehicleMap));
            itcastDataObj.setVcuBrkErr(convertStringType("VcuBrkErr", vehicleMap));
            itcastDataObj.setEPB_AchievedClampForce(convertStringType("EPB_AchievedClampForce", vehicleMap));
            itcastDataObj.setEpbSwitchPosition(convertStringType("epbSwitchPosition", vehicleMap));
            itcastDataObj.setEpbStatus(convertStringType("epbStatus", vehicleMap));
            itcastDataObj.setEspActiveStatus(convertStringType("espActiveStatus", vehicleMap));
            itcastDataObj.setEspFunctionStatus(convertStringType("espFunctionStatus", vehicleMap));
            itcastDataObj.setESP_TCSFailStatus(convertStringType("ESP_TCSFailStatus", vehicleMap));
            itcastDataObj.setHhcActive(convertStringType("hhcActive", vehicleMap));
            itcastDataObj.setTcsActive(convertStringType("tcsActive", vehicleMap));
            itcastDataObj.setEspMasterCylinderBrakePressure(convertStringType("espMasterCylinderBrakePressure", vehicleMap));
            itcastDataObj.setESP_MasterCylinderBrakePressureValid(convertStringType("ESP_MasterCylinderBrakePressureValid", vehicleMap));
            itcastDataObj.setEspTorqSensorStatus(convertStringType("espTorqSensorStatus", vehicleMap));
            itcastDataObj.setEPS_EPSFailed(convertStringType("EPS_EPSFailed", vehicleMap));
            itcastDataObj.setSasFailure(convertStringType("sasFailure", vehicleMap));
            itcastDataObj.setSasSteeringAngleSpeed(convertStringType("sasSteeringAngleSpeed", vehicleMap));
            itcastDataObj.setSasSteeringAngle(convertStringType("sasSteeringAngle", vehicleMap));
            itcastDataObj.setSasSteeringAngleValid(convertStringType("sasSteeringAngleValid", vehicleMap));
            itcastDataObj.setEspSteeringTorque(convertStringType("espSteeringTorque", vehicleMap));
            itcastDataObj.setAcReq(convertIntType("acReq", vehicleMap));
            itcastDataObj.setAcSystemFailure(convertIntType("acSystemFailure", vehicleMap));
            itcastDataObj.setPtcPwrAct(convertDoubleType("ptcPwrAct", vehicleMap));
            itcastDataObj.setPlasmaStatus(convertIntType("plasmaStatus", vehicleMap));
            itcastDataObj.setBattInTemperature(convertIntType("battInTemperature", vehicleMap));
            itcastDataObj.setBattWarmLoopSts(convertStringType("battWarmLoopSts", vehicleMap));
            itcastDataObj.setBattCoolngLoopSts(convertStringType("battCoolngLoopSts", vehicleMap));
            itcastDataObj.setBattCoolActv(convertStringType("battCoolActv", vehicleMap));
            itcastDataObj.setMotorOutTemperature(convertIntType("motorOutTemperature", vehicleMap));
            itcastDataObj.setPowerStatusFeedBack(convertStringType("powerStatusFeedBack", vehicleMap));
            itcastDataObj.setAC_RearDefrosterSwitch(convertIntType("AC_RearDefrosterSwitch", vehicleMap));
            itcastDataObj.setRearFoglamp(convertIntType("rearFoglamp", vehicleMap));
            itcastDataObj.setDriverDoorLock(convertIntType("driverDoorLock", vehicleMap));
            itcastDataObj.setAcDriverReqTemp(convertDoubleType("acDriverReqTemp", vehicleMap));
            itcastDataObj.setKeyAlarm(convertIntType("keyAlarm", vehicleMap));
            itcastDataObj.setAirCleanStsRemind(convertIntType("airCleanStsRemind", vehicleMap));
            itcastDataObj.setRecycleType(convertIntType("recycleType", vehicleMap));
            itcastDataObj.setStartControlsignal(convertStringType("startControlsignal", vehicleMap));
            itcastDataObj.setAirBagWarningLamp(convertIntType("airBagWarningLamp", vehicleMap));
            itcastDataObj.setFrontDefrosterSwitch(convertIntType("frontDefrosterSwitch", vehicleMap));
            itcastDataObj.setFrontBlowType(convertStringType("frontBlowType", vehicleMap));
            itcastDataObj.setFrontReqWindLevel(convertIntType("frontReqWindLevel", vehicleMap));
            itcastDataObj.setBcmFrontWiperStatus(convertStringType("bcmFrontWiperStatus", vehicleMap));
            itcastDataObj.setTmsPwrAct(convertStringType("tmsPwrAct", vehicleMap));
            itcastDataObj.setKeyUndetectedAlarmSign(convertIntType("keyUndetectedAlarmSign", vehicleMap));
            itcastDataObj.setPositionLamp(convertStringType("positionLamp", vehicleMap));
            itcastDataObj.setDriverReqTempModel(convertIntType("driverReqTempModel", vehicleMap));
            itcastDataObj.setTurnLightSwitchSts(convertIntType("turnLightSwitchSts", vehicleMap));
            itcastDataObj.setAutoHeadlightStatus(convertIntType("autoHeadlightStatus", vehicleMap));
            itcastDataObj.setDriverDoor(convertIntType("driverDoor", vehicleMap));
            itcastDataObj.setFrntIpuFltRnk(convertIntType("frntIpuFltRnk", vehicleMap));
            itcastDataObj.setFrontIpuSwVers(convertStringType("frontIpuSwVers", vehicleMap));
            itcastDataObj.setFrontIpuHwVers(convertIntType("frontIpuHwVers", vehicleMap));
            itcastDataObj.setFrntMotTqLongTermMax(convertIntType("frntMotTqLongTermMax", vehicleMap));
            itcastDataObj.setFrntMotTqLongTermMin(convertIntType("frntMotTqLongTermMin", vehicleMap));
            itcastDataObj.setCpvValue(convertIntType("cpvValue", vehicleMap));
            itcastDataObj.setObcChrgSts(convertIntType("obcChrgSts", vehicleMap));
            itcastDataObj.setObcFltRnk(convertStringType("obcFltRnk", vehicleMap));
            itcastDataObj.setObcChrgInpAcI(convertDoubleType("obcChrgInpAcI", vehicleMap));
            itcastDataObj.setObcChrgInpAcU(convertIntType("obcChrgInpAcU", vehicleMap));
            itcastDataObj.setObcChrgDcI(convertDoubleType("obcChrgDcI", vehicleMap));
            itcastDataObj.setObcChrgDcU(convertDoubleType("obcChrgDcU", vehicleMap));
            itcastDataObj.setObcTemperature(convertIntType("obcTemperature", vehicleMap));
            itcastDataObj.setObcMaxChrgOutpPwrAvl(convertIntType("obcMaxChrgOutpPwrAvl", vehicleMap));
            itcastDataObj.setPassengerBuckleSwitch(convertIntType("passengerBuckleSwitch", vehicleMap));
            itcastDataObj.setCrashlfo(convertStringType("crashlfo", vehicleMap));
            itcastDataObj.setDriverBuckleSwitch(convertIntType("driverBuckleSwitch", vehicleMap));
            itcastDataObj.setEngineStartHibit(convertStringType("engineStartHibit", vehicleMap));
            itcastDataObj.setLockCommand(convertStringType("lockCommand", vehicleMap));
            itcastDataObj.setSearchCarReq(convertStringType("searchCarReq", vehicleMap));
            itcastDataObj.setAcTempValueReq(convertStringType("acTempValueReq", vehicleMap));
            itcastDataObj.setVcuErrAmnt(convertStringType("vcuErrAmnt", vehicleMap));
            itcastDataObj.setVcuSwVers(convertIntType("vcuSwVers", vehicleMap));
            itcastDataObj.setVcuHwVers(convertIntType("vcuHwVers", vehicleMap));
            itcastDataObj.setLowSpdWarnStatus(convertStringType("lowSpdWarnStatus", vehicleMap));
            itcastDataObj.setLowBattChrgRqe(convertIntType("lowBattChrgRqe", vehicleMap));
            itcastDataObj.setLowBattChrgSts(convertStringType("lowBattChrgSts", vehicleMap));
            itcastDataObj.setLowBattU(convertDoubleType("lowBattU", vehicleMap));
            itcastDataObj.setHandlebrakeStatus(convertIntType("handlebrakeStatus", vehicleMap));
            itcastDataObj.setShiftPositionValid(convertStringType("shiftPositionValid", vehicleMap));
            itcastDataObj.setAccPedalValid(convertStringType("accPedalValid", vehicleMap));
            itcastDataObj.setDriveMode(convertIntType("driveMode", vehicleMap));
            itcastDataObj.setDriveModeButtonStatus(convertIntType("driveModeButtonStatus", vehicleMap));
            itcastDataObj.setVCUSRSCrashOutpSts(convertIntType("VCUSRSCrashOutpSts", vehicleMap));
            itcastDataObj.setTextDispEna(convertIntType("textDispEna", vehicleMap));
            itcastDataObj.setCrsCtrlStatus(convertIntType("crsCtrlStatus", vehicleMap));
            itcastDataObj.setCrsTarSpd(convertIntType("crsTarSpd", vehicleMap));
            itcastDataObj.setCrsTextDisp(convertIntType("crsTextDisp",vehicleMap ));
            itcastDataObj.setKeyOn(convertIntType("keyOn", vehicleMap));
            itcastDataObj.setVehPwrlim(convertIntType("vehPwrlim", vehicleMap));
            itcastDataObj.setVehCfgInfo(convertStringType("vehCfgInfo", vehicleMap));
            itcastDataObj.setVacBrkPRmu(convertIntType("vacBrkPRmu", vehicleMap));

            /**
             * 解析复杂数据结构：
             * 1：nevChargeSystemVoltageDtoList
             * 2：nevChargeSystemTemperatureDtoList
             * 3：driveMotorData
             * 4：xcuerrinfo
             * */
            //1：nevChargeSystemVoltageDtoList:可充电储能子系统电压信息列表
            List<HashMap<String, Object>> nevChargeSystemVoltageDtoList = jsonStrToList(vehicleMap.getOrDefault("nevChargeSystemVoltageDtoList", new ArrayList<Object>()).toString());
            if(!nevChargeSystemVoltageDtoList.isEmpty()){
                //解析list中的的第一个map对象集合，集合中的第一条数据为有效数据
                HashMap<String, Object> nevChargeSystemVoltageDToMap = nevChargeSystemVoltageDtoList.get(0);
                itcastDataObj.setCurrentBatteryStartNum(convertIntType("currentBatteryStartNum", nevChargeSystemVoltageDToMap));
                itcastDataObj.setChargeSystemVoltage(convertDoubleType("chargeSystemVoltage", nevChargeSystemVoltageDToMap));
                itcastDataObj.setCurrentBatteryCount(convertIntType("currentBatteryCount", nevChargeSystemVoltageDToMap));
                itcastDataObj.setBatteryCount(convertIntType("batteryCount", nevChargeSystemVoltageDToMap));
                itcastDataObj.setChildSystemNum(convertIntType("childSystemNum", nevChargeSystemVoltageDToMap));
                itcastDataObj.setChargeSystemCurrent(convertDoubleType("chargeSystemCurrent", nevChargeSystemVoltageDToMap));
                itcastDataObj.setBatteryVoltage(convertJoinStringType("batteryVoltage", nevChargeSystemVoltageDToMap));
            }

            //2：nevChargeSystemTemperatureDtoList
            List<HashMap<String, Object>> nevChargeSystemTemperatureDtoList = jsonStrToList(vehicleMap.getOrDefault("nevChargeSystemTemperatureDtoList", new ArrayList()).toString());
            if (!nevChargeSystemTemperatureDtoList.isEmpty()) {
                HashMap<String, Object> nevChargeSystemTemperatureMap = nevChargeSystemTemperatureDtoList.get(0);
                itcastDataObj.setProbeTemperatures(convertJoinStringType("probeTemperatures", nevChargeSystemTemperatureMap));
                itcastDataObj.setChargeTemperatureProbeNum(convertIntType("chargeTemperatureProbeNum", nevChargeSystemTemperatureMap));
            }

            //3：driveMotorData
            List<HashMap<String, Object>> driveMotorData = jsonStrToList(vehicleMap.getOrDefault("driveMotorData", new ArrayList()).toString());                                    //驱动电机数据
            if (!driveMotorData.isEmpty()) {
                HashMap<String, Object> driveMotorMap = driveMotorData.get(0);
                itcastDataObj.setControllerInputVoltage(convertDoubleType("controllerInputVoltage", driveMotorMap));
                itcastDataObj.setControllerTemperature(convertDoubleType("controllerTemperature", driveMotorMap));
                itcastDataObj.setRevolutionSpeed(convertDoubleType("revolutionSpeed", driveMotorMap));
                itcastDataObj.setNum(convertIntType("num", driveMotorMap));
                itcastDataObj.setControllerDcBusCurrent(convertDoubleType("controllerDcBusCurrent", driveMotorMap));
                itcastDataObj.setTemperature(convertDoubleType("temperature", driveMotorMap));
                itcastDataObj.setTorque(convertDoubleType("torque", driveMotorMap));
                itcastDataObj.setState(convertIntType("state", driveMotorMap));
            }

            //4：xcuerrinfo
            HashMap<String, Object> xcuerrinfoMap = jsonStrToMap(vehicleMap.getOrDefault("xcuerrinfo", new HashMap<String, Object>()).toString());
            if (!xcuerrinfoMap.isEmpty()) {
                List<HashMap<String, Object>> ecuErrCodeDataList = jsonStrToList(xcuerrinfoMap.getOrDefault("ecuErrCodeDataList", new ArrayList()).toString()) ;
                if (ecuErrCodeDataList.size() > 4) {
                    itcastDataObj.setVcuFaultCode(convertJoinStringType("errCodes", ecuErrCodeDataList.get(0)));
                    itcastDataObj.setBcuFaultCodes(convertJoinStringType("errCodes", ecuErrCodeDataList.get(1)));
                    itcastDataObj.setDcdcFaultCode(convertJoinStringType("errCodes", ecuErrCodeDataList.get(2)));
                    itcastDataObj.setIpuFaultCodes(convertJoinStringType("errCodes", ecuErrCodeDataList.get(3)));
                    itcastDataObj.setObcFaultCode(convertJoinStringType("errCodes", ecuErrCodeDataList.get(4)));
                }
            }

            //TODO 区分出来转换成javaBean后的数据是正常数据还是异常数据（在javaBean中进行判断比较方便，后续直接使用即可）
            if(StringUtils.isEmpty(itcastDataObj.getVin()) || StringUtils.isEmpty(itcastDataObj.getTerminalTime())){
                //异常数据
                if(StringUtils.isNotEmpty(jsonStr)){
                    //扩展字段，用于存储异常数据
                    itcastDataObj.setErrorData(jsonStr);
                }

                //打印关键信息
                if(StringUtils.isEmpty(itcastDataObj.getVin())){
                    logger.error("原始数据中的vin为空："+jsonStr);
                }
                if(StringUtils.isEmpty(itcastDataObj.getTerminalTime())){
                    logger.error("原始数据中的TerminalTime为空："+jsonStr);
                }
            }

            //目前TerminalTime字段的值是字符串类型，这个字段的意思相当于EventTime，因此在后续如果添加水印需要指定事件时间，而指定事件时间的字段类型必须是long类型（timestamp）
            //TODO 扩展字段赋值：将终端时间（字符串类型转换成long类型）
            if(StringUtils.isNotEmpty(itcastDataObj.getTerminalTime())){
                itcastDataObj.setTerminalTimeStamp(DateUtil.convertStringToDate(itcastDataObj.getTerminalTime()).getTime());
            }
        } catch (Exception exception) {
            //异常数据
            if(!org.apache.commons.lang.StringUtils.isEmpty(jsonStr)){
                itcastDataObj.setErrorData(jsonStr);
            }
            logger.error(exception.getMessage());
            //exception.printStackTrace();
        }
        //返回转换后的javaBean对象
        return  itcastDataObj;
    }

    /**
     * 将json字符串转换成list对象
     * @param jsonStr
     * @return
     */
    private static List<HashMap<String, Object>> jsonStrToList(String jsonStr) {
        //定义需要返回的list对象
        List<HashMap<String, Object>> resultList = new ArrayList<>();
        //定义jsonArray对象
        JSONArray jsonArray = new JSONArray(jsonStr);
        for (int i = 0; i < jsonArray.length(); i++) {
            HashMap<String, Object> hashMap = jsonStrToMap(jsonArray.get(i).toString());
            resultList.add(hashMap);
        }

        //返回json列表集合对象
        return resultList;
    }

    /**
     * 将json字符串转换成map对象
     * @param jsonStr
     * @return
     */
    private static HashMap<String, Object> jsonStrToMap(String jsonStr) {
        //创建jsonObject对象
        JSONObject jsonObject = new JSONObject(jsonStr);
        //定义需要返回的map对象
        HashMap<String, Object> hashMap = new HashMap<>();
        //获取到所有的key集合
        Iterator<String> iterator = jsonObject.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            Object value = jsonObject.get(key);
            hashMap.put(key, value);
        }

        //返回map对象
        return  hashMap;
    }

    /**
     * 根据字段名获取字段值，并将字段值转换成int类型返回
     * @param key
     * @param vihicleMap
     * @return
     */
    private static int convertIntType(String key, HashMap<String, Object> vihicleMap) {
        return Integer.parseInt(vihicleMap.getOrDefault(key, -999999).toString());
    }

    /**
     * 根据字段名获取字段值，并将字段值转换成字符串类型返回
     * @param key
     * @param vihicleMap
     * @return
     */
    private static String convertStringType(String key, HashMap<String, Object> vihicleMap){
        return vihicleMap.getOrDefault(key, "").toString();
    }

    /**
     * 根据字段名获取字段值，并将字段值转换成double类型返回
     * @param key
     * @param vihicleMap
     * @return
     */
    private static Double convertDoubleType(String key, HashMap<String, Object> vihicleMap){
        return Double.parseDouble(vihicleMap.getOrDefault(key, -999999).toString());
    }

    /**
     * 使用指定的分割符，将集合中的所有的元素进行拼接起来
     * @param key
     * @param hashMap
     * @return
     */
    private static String convertJoinStringType(String key, HashMap<String, Object> hashMap) {
        return  String.join("-", convertStringToArray(hashMap.getOrDefault(key, "").toString()));
    }

    /**
     * 将字符串拆分成列表对象返回
     * @param str
     * @return
     */
    private static  List convertStringToArray(String str) {return Arrays.asList(str.split(","));}

}
