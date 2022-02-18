package cn.itcast.json.bean;

/**
 * 根据json字符串格式定义javaBean对象
 * {"batteryAlarm": 0, "carMode": 1,"minVoltageBattery": 3.89, "chargeStatus": 1,"vin":" LS5A3CJC0JF890971"}
 */
public class CarJsonBean {
    private int batteryAlarm;
    private int carMode;
    private double minVoltageBattery;
    private int chargeStatus;
    private String vin;


    public CarJsonBean(int batteryAlarm, int carMode, double minVoltageBattery, int chargeStatus, String vin) {
        this.batteryAlarm = batteryAlarm;
        this.carMode = carMode;
        this.minVoltageBattery = minVoltageBattery;
        this.chargeStatus = chargeStatus;
        this.vin = vin;
    }

    @Override
    public String toString() {
        return "CarJsonBean{" +
                "batteryAlarm=" + batteryAlarm +
                ", carMode=" + carMode +
                ", minVoltageBattery=" + minVoltageBattery +
                ", chargeStatus=" + chargeStatus +
                ", vin='" + vin + '\'' +
                '}';
    }

    public int getBatteryAlarm() {
        return batteryAlarm;
    }

    public void setBatteryAlarm(int batteryAlarm) {
        this.batteryAlarm = batteryAlarm;
    }

    public int getCarMode() {
        return carMode;
    }

    public void setCarMode(int carMode) {
        this.carMode = carMode;
    }

    public double getMinVoltageBattery() {
        return minVoltageBattery;
    }

    public void setMinVoltageBattery(double minVoltageBattery) {
        this.minVoltageBattery = minVoltageBattery;
    }

    public int getChargeStatus() {
        return chargeStatus;
    }

    public void setChargeStatus(int chargeStatus) {
        this.chargeStatus = chargeStatus;
    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }
}
