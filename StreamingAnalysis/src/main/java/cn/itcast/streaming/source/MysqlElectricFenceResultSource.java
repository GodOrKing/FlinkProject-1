package cn.itcast.streaming.source;

import cn.itcast.entity.ElectricFenceResultTmp;
import cn.itcast.utils.DateUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * 读取电子围栏分析结果表的数据，并进行广播
 */
public class MysqlElectricFenceResultSource extends RichSourceFunction<HashMap<String, Long>> {
    private static Logger logger = LoggerFactory.getLogger(MysqlElectricFenceResultSource.class);
    //定义全局的参数对象
    private ParameterTool parameterTool;
    //定义connection的连接对象
    private Connection connection;
    //定义statment的对象
    private PreparedStatement statement;
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
        String sql = "select vin, min(id) id\n" +
                "from vehicle_networking.electric_fence where inTime is not null and outTime is null GROUP BY vin;";
        //实例化statement
        statement = connection.prepareStatement(sql);
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
    public void run(SourceContext<HashMap<String, Long>> sourceContext) throws Exception {
        while (isRunning){
            //构建返回的HashMap对象
            HashMap<String, Long> vehicleMapInfo = new HashMap<>();
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()){
                vehicleMapInfo.put(resultSet.getString("vin"), resultSet.getLong("id"));
            }
            if(vehicleMapInfo.isEmpty()){
                logger.warn("从mysql中没有获取到电子围栏解析结果表的数据...");
            }else{
                logger.warn("从mysql中获取到电子围栏解析结果表的数据："+vehicleMapInfo.size());
            }
            System.out.println("电子围栏历史分析结果表数据："+vehicleMapInfo);
            resultSet.close();

            //返回数据
            sourceContext.collect(vehicleMapInfo);
            //这个时间戳的数据一定要小于窗口滚动的时间，否则进入电子围栏数据写入mysql以后，广播流的数据没有来的及更新，导致匹配更新数据失败
            TimeUnit.SECONDS.sleep(1);
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
