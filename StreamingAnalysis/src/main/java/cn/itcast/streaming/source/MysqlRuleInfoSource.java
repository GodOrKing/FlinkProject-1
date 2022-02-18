package cn.itcast.streaming.source;

import cn.itcast.entity.RuleInfoModel;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * 实现读取告警规则的数据源
 */
public class MysqlRuleInfoSource extends RichSourceFunction<ArrayList<RuleInfoModel>> {

    private static Logger logger = LoggerFactory.getLogger(MysqlRuleInfoSource.class);
    //定义全局的参数对象
    private ParameterTool parameterTool;
    //定义connection的连接对象
    private Connection connection;
    //定义statment的对象
    private PreparedStatement statement;
    //定义是否u运行标记
    private boolean isRunning = true;

    /**
     * 初始化mysql连接对象
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //注册驱动
        Class.forName(parameterTool.getRequired("jdbc.driver"));
        //实例化mysql的连接参数
        String url = parameterTool.getRequired("jdbc.url");
        //mysql的用户名
        String user = parameterTool.getRequired("jdbc.user");
        //mysql的密码
        String password = parameterTool.getRequired("jdbc.password");
        connection = DriverManager.getConnection(url, user, password);
        //实例化statement
        statement = connection.prepareStatement("SELECT t.rule_name, t.alarm_param1_field, t.operator1, t.alarm_param2_field, t.rule_symbol1, t.alarm_threshold1, t.logical_symbol, t.rule_symbol2, t.alarm_threshold2, \n" +
                "t.alarm_param3_field, t.operator2, t.alarm_param4_field, t.rule_symbol3, t.alarm_threshold3, t.logical_symbol2, t.rule_symbol4, t.alarm_threshold4, t.alarm_frame, \n" +
                "t1.monitor_type_id, t1.series_name, t1.series_code, t1.model_code, t1.model_name, t1.province, t1.city, t1.vins, t1.id, t2.monitor_type_name \n" +
                "FROM vehicle_networking.t_alarm_rule t \n" +
                "JOIN vehicle_networking.t_monitor_task t1 ON t1.alarm_rule_id = t.id \n" +
                "JOIN vehicle_networking.t_monitor_type t2 ON t2.id = t1.monitor_type_id \n" +
                "WHERE t1.STATUS = 1 AND t.STATUS =1");
    }

    /**
     * 关闭连接释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if(statement!=null && !statement.isClosed()) statement.close();
        if(connection != null && !connection.isClosed()) connection.close();
    }

    /**
     * 数据读取方法
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<ArrayList<RuleInfoModel>> sourceContext) throws Exception {
        //定义需要返回的ArrayList对象
        ArrayList<RuleInfoModel> ruleInfoList = new ArrayList<>();
        while (isRunning) {
            ResultSet resultSet = statement.executeQuery();
            //删除历史的数据
            ruleInfoList.clear();
            while (resultSet.next()){
                RuleInfoModel ruleInfoModel = new RuleInfoModel();
                ruleInfoModel.setRuleName(resultSet.getString("rule_name"));//1
                ruleInfoModel.setAlarmFrame(resultSet.getString("alarm_frame"));//2
                ruleInfoModel.setMonitorTypeId(resultSet.getString("monitor_type_id"));//3
                ruleInfoModel.setSeriesName(resultSet.getString("series_name"));//4
                ruleInfoModel.setSeriesCode(resultSet.getString("series_code"));//5
                ruleInfoModel.setModelCode(resultSet.getString("model_code"));//6
                ruleInfoModel.setModelName(resultSet.getString("model_name"));//7
                ruleInfoModel.setProvince(resultSet.getString("province"));//8
                ruleInfoModel.setCity(resultSet.getString("city"));//9
                ruleInfoModel.setVins(resultSet.getString("vins"));//10
                ruleInfoModel.setMonitorTypeName(resultSet.getString("monitor_type_name"));//11
                ruleInfoModel.setId(resultSet.getString("id"));//12
                //表达式一 包含自定义告警字段1、自定义告警字段2、操作符1、规则符号1、告警阈值1、逻辑符号、规则符号2、告警阈值2
                ruleInfoModel.setAlarmParam1Field(resultSet.getString("alarm_param1_field"));//13
                ruleInfoModel.setAlarmParam2Field(resultSet.getString("alarm_param2_field"));//18
                ruleInfoModel.setOperator1(resultSet.getString("operator1"));//14
                ruleInfoModel.setRuleSymbol1(resultSet.getString("rule_symbol1"));//15
                ruleInfoModel.setAlarmThreshold1(resultSet.getString("alarm_threshold1"));//16
                ruleInfoModel.setLogicalSymbol(resultSet.getString("logical_symbol"));//17
                ruleInfoModel.setRuleSymbol2(resultSet.getString("rule_symbol2"));//19
                ruleInfoModel.setAlarmThreshold2(resultSet.getString("alarm_threshold2"));//20
                //表达式二 包含自定义告警字段3、自定义告警字段4、操作符2、规则符号3、告警阈值3、逻辑符号2、规则符号4、告警阈值4
                ruleInfoModel.setAlarmParam3Field(resultSet.getString("alarm_param3_field"));//23
                ruleInfoModel.setAlarmParam4Field(resultSet.getString("alarm_param4_field"));//26
                ruleInfoModel.setOperator2(resultSet.getString("operator2"));//21
                ruleInfoModel.setRuleSymbol3(resultSet.getString("rule_symbol3"));//24
                ruleInfoModel.setAlarmThreshold3(resultSet.getString("alarm_threshold3"));//25
                ruleInfoModel.setLogicalSymbol2(resultSet.getString("logical_symbol2"));//22
                ruleInfoModel.setRuleSymbol4(resultSet.getString("rule_symbol4"));//27
                ruleInfoModel.setAlarmThreshold4(resultSet.getString("alarm_threshold4"));//28

                ruleInfoList.add(ruleInfoModel);
            }

            if(ruleInfoList.isEmpty()){
                System.out.println("没有获取到自定义告警规则...");
            }else{
                System.out.println("自定义告警规则："+ruleInfoList);
            }
            sourceContext.collect(ruleInfoList);

            //设置数据读取的间隔
            TimeUnit.SECONDS.sleep(Long.parseLong(parameterTool.getRequired("ruleinfo.millionseconds")));
        }
    }

    /**
     * 取消，停止数据的获取
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
