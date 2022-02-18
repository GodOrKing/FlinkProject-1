package cn.itcast.streaming.source;

import cn.itcast.entity.ElectricFenceResultTmp;
import cn.itcast.utils.DateUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * 读取mysql存储的电子围栏规则表数据以及电子围栏规则关联的电子围栏规则车辆表数据
 * 根据分析，一个车辆可能适配多个电子围栏规则，所以返回的数据类型定义为HashMap<vin, 电子围栏规则对象>
 * 为了方便处理，我们只处理一个车辆关联一个电子围栏规则的场景（真事的业务开发中一定是一个车辆可能有很多很多对应电子围栏规则的）
 *
 */
public class MysqlElectricFenceSouce extends RichSourceFunction<HashMap<String, ElectricFenceResultTmp>> {
    private static Logger logger = LoggerFactory.getLogger(MysqlElectricFenceSouce.class);
    //定义全局的参数对象
    private ParameterTool parameterTool;
    //定义connection的连接对象
    private Connection connection;
    //定义statment的对象
    private Statement statement;
    //定义是否u运行标记
    private boolean isRunning = true;
    /**
     * 打开mysql连接，初始化资源
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
        statement = connection.createStatement();
    }

    /**
     * 关闭连接，释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if(statement!= null && !statement.isClosed()) statement.close();
        if(connection!= null && !connection.isClosed()) connection.close();
    }

    /**
     * 数据读取方法
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<HashMap<String, ElectricFenceResultTmp>> sourceContext) throws Exception {
        while (isRunning){
            //构建返回的HashMap对象
            HashMap<String, ElectricFenceResultTmp> electricFenceResult = new HashMap<>();
            ResultSet resultSet = statement.executeQuery("select vin,name,address,radius,longitude,latitude,start_time,end_time,setting.id\n" +
                    "from vehicle_networking.electronic_fence_vins vin\n" +
                    "INNER JOIN vehicle_networking.electronic_fence_setting setting\n" +
                    "ON   vin.setting_id=setting.id and setting.`status`=1;");
            while (resultSet.next()){
                electricFenceResult.put(resultSet.getString("vin"),
                        new ElectricFenceResultTmp(
                                resultSet.getInt("id"),
                                resultSet.getString("name"),
                                resultSet.getString("address"),
                                resultSet.getFloat("radius"),
                                resultSet.getDouble("longitude"),
                                resultSet.getDouble("latitude"),
                                DateUtil.convertDateStrToDate(resultSet.getString("start_time")) ,
                                DateUtil.convertDateStrToDate(resultSet.getString("end_time"))
                ));
            }
            if(electricFenceResult.isEmpty()){
                logger.warn("从mysql中没有获取到电子围栏规则表的数据...");
            }else{
                logger.warn("从mysql中获取到电子围栏规则表的数据："+electricFenceResult.size());
            }
            System.out.println("电子围栏规则数据："+electricFenceResult);
            resultSet.close();

            //返回数据
            sourceContext.collect(electricFenceResult);
            TimeUnit.SECONDS.sleep(Long.parseLong(parameterTool.getRequired("vehinfo.millionseconds")));
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
