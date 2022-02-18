package cn.itcast.dataservice.bean;

import lombok.Getter;
import lombok.Setter;

/**
 * @Auther: laowei
 * @Date: 2020/9/16 1:46
 * @Description: TODO 后台数据服务接口热力图对象
 */
@Getter
@Setter
public class HeatMapPointBean {
    // todo 经度
    private double longitude;
    // todo 纬度
    private double latitude;
    // todo 海拔
    // private float elevation;
    // todo 城市
    // private String city;
}
