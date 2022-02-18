package cn.itcast.dataservice.bean;

import lombok.Getter;
import lombok.Setter;

/**
 * @Auther: laowei
 * @Date: 2020/9/16 1:45
 * @Description: TODO 电子围栏数据服务接口对象
 */
@Getter
@Setter
public class ElectricFenceBean {
    /** todo 车架号 */
    private String vin;
    /** todo 进电子围栏时间 */
    private String inTime;
    /** todo 出电子围栏时间 */
    private String outTime;
    /** todo 位置时间 */
    private String gpsTime;
    /** todo 位置纬度 */
    private Double lat;
    /** todo 位置经度 */
    private Double lng;
    /** todo 电子围栏ID */
    private int eleId;
    /** todo 电子围栏名称 */
    private String eleName;
    /** todo 中心点地址 */
    private String address;
    /** todo 中心点纬度 */
    private Double latitude;
    /** todo 中心点经度 */
    private Double longitude;
    /** todo 电子围栏半径 */
    private Float radius;
    /** todo 终端时间 */
    private String terminalTime;
    /** todo 插入数据的时间 */
    private String processTime;
}