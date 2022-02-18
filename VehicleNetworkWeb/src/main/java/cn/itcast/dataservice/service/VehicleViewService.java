package cn.itcast.dataservice.service;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/17 23:09
 * @Description TODO 车辆可视化页面服务
 */
public interface VehicleViewService {
    // todo 统计平台车辆总数
    Long totalNum();
    // todo 统计平台在线车辆
    Long onlineNum();
    // todo 统计平台行驶中的车辆
    Integer drivingNum();
}