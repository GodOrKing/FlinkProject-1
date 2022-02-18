package cn.itcast.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 定义自定义告警规则分析结果的javaBean对象
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomRuleAlarmResultModel {
    //定义车辆的唯一编号
    private String vin = "";
    //终端时间
    private String terminalTime = "";
    //定义状态标识为1：表示触发报警规则，0：表示没有触发报警规则
    private int alarmFlag;
    //定义告警的帧数值
    private int alarmFrame;
    //自定义告警规则名称
    private String ruleName;
    //车系名称
    private String seriesNameValue = "未知";
    //车系编码
    private String seriesCodeValue = "未知";
    //车型名称
    private String modelNameValue = "未知";
    //车型编码
    private String modelCodeValue = "未知";
    //省份
    private String province;
    //城市
    private String city;
    //国家
    private String county;
    //区县
    private String district;
    //详细地址
    private String address;
    //纬度
    private Double lat = -999999D;
    //经度
    private Double lng = -999999D;
    //监控id
    private int monitorId;
}
