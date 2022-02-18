package cn.itcast.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * 定义地理位置javaBean对象
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VehicleLocationModel implements Serializable {
    //省份
    private String province;
    //城市
    private String city;
    //国家
    private String country;
    //区县
    private String district;
    //详细地址
    private String address;
    //纬度
    private Double lat = -999999D;
    //经度
    private Double lng = -999999D;
}
