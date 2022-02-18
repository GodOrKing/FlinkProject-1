package cn.itcast.entity;

import lombok.Data;

/**
 * 定义自定义告警规则的javaBean对象
 * 将该自定义告警规则赋值以后应用到数据流对象中
 */
@Data
public class RuleInfoModel {
    //自定义规则的名称
    private String ruleName;
    //告警帧数值：数据每秒刷新次数
    private String alarmFrame;
    //监控任务类型id
    private String monitorTypeId;
    //车系名称
    private String seriesName;
    //车系编码
    private String seriesCode;
    //车型编码
    private String modelCode;
    //车型名称
    private String modelName;
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
    //告警任务监控的车辆，多个车辆以逗号进行拼接
    private String vins;
    //监控任务类型名称
    private String monitorTypeName;
    //监控任务id
    private String id;

    //告警字段1
    private String alarmParam1Field;
    //告警字段1操作符：+ - * /
    private String operator1;
    //告警字段1规则符号：> >= < <= !=
    private String ruleSymbol1;
    //告警临界值1(阈值)，对应告警字段1
    private String alarmThreshold1;
    //告警字段1对应逻辑运算符符号1
    private String logicalSymbol;

    //告警字段2
    private String alarmParam2Field;
    //告警字段2操作符：+ - * /
    private String operator2;
    //告警字段2规则符号：> >= < <= !=
    private String ruleSymbol2;
    //告警临界值2(阈值)，对应告警字段2
    private String alarmThreshold2;
    //告警字段2对应逻辑运算符符号2
    private String logicalSymbol2;

    //告警字段3
    private String alarmParam3Field;
    //告警字段3操作符：+ - * /
    private String operator3;
    //告警字段3规则符号：> >= < <= !=
    private String ruleSymbol3;
    //告警临界值3(阈值)，对应告警字段3
    private String alarmThreshold3;
    //告警字段3对应逻辑运算符符号3
    private String logicalSymbol3;

    //告警字段4
    private String alarmParam4Field;
    //告警字段4操作符：+ - * /
    private String operator4;
    //告警字段4规则符号：> >= < <= !=
    private String ruleSymbol4;
    //告警临界值4(阈值)，对应告警字段4
    private String alarmThreshold4;
    //告警字段4对应逻辑运算符符号4
    private String logicalSymbol4;
}
