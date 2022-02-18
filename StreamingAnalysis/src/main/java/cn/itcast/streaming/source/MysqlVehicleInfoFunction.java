package cn.itcast.streaming.source;

import cn.itcast.entity.VehicleInfoModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * 自定义实现车辆基础信息表的加载
 * 加载车辆基础信息表（车辆类型、车辆、销售记录表、车辆用途表）
 */
public class MysqlVehicleInfoFunction extends RichSourceFunction<HashMap<String, VehicleInfoModel>> {

    private static Logger logger = LoggerFactory.getLogger(MysqlVehicleInfoFunction.class);
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
        String sql = "select t12.vin,t12.series_name,t12.model_name,t12.series_code,t12.model_code,t12.nick_name,t3.sales_date sales_date,t4.car_type\n" +
                " from (\n" +
                "select t1.vin, t1.series_name, t2.show_name as model_name, t1.series_code,t2.model_code,t2.nick_name,t1.vehicle_id\n" +
                " from vehicle_networking.dcs_vehicles t1 \n" +
                " left join vehicle_networking.t_car_type_code t2 on t1.model_code = t2.model_code) t12\n" +
                " left join  (select vehicle_id, max(sales_date) sales_date from vehicle_networking.dcs_sales group by vehicle_id) t3\n" +
                " on t12.vehicle_id = t3.vehicle_id\n" +
                " left join\n" +
                " (select tc.vin,'net_cat' car_type from vehicle_networking.t_net_car tc\n" +
                " union all select tt.vin,'taxi' car_type from vehicle_networking.t_taxi tt\n" +
                " union all select tp.vin,'private_car' car_type from vehicle_networking.t_private_car tp\n" +
                " union all select tm.vin,'model_car' car_type from vehicle_networking.t_model_car tm) t4\n" +
                " on t12.vin = t4.vin";
        statement = connection.prepareStatement(sql);
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
     * 读取数据
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<HashMap<String, VehicleInfoModel>> sourceContext) throws Exception {
        while (isRunning){
            ResultSet resultSet = statement.executeQuery();
            HashMap<String, VehicleInfoModel> resultMap = new HashMap<>();
            while (resultSet.next()){
                //车架号
                String vin = resultSet.getString("vin");
                //车系code
                String seriesCode = resultSet.getString("series_code");
                //车系名称
                String seriesName = resultSet.getString("series_name");
                //车型code
                String modelCode = resultSet.getString("model_code");
                //车型名称
                String modelName = resultSet.getString("model_name");
                //简称
                String nickName = resultSet.getString("nick_name");
                //出售日期
                String salesDate = resultSet.getString("sales_date");
                //车辆用途
                String carType = resultSet.getString("car_type");
                //年限
                String liveTime = null;
                if(StringUtils.isNotEmpty(salesDate)){
                    //当前日期-出售日期=使用年限
                    liveTime = String.valueOf((new Date().getTime() - resultSet.getDate("sales_date").getTime())/1000/3600/24/365);
                }
                //如果字段为空，赋予默认值
                if(vin == null) vin = "未知";
                if(seriesCode == null) seriesCode = "未知";
                if(seriesName == null) seriesName = "未知";
                if(modelCode == null) modelCode = "未知";
                if(modelName == null) modelName = "未知";
                if(nickName == null) nickName = "未知";
                if(salesDate == null) salesDate = "未知";
                if(carType == null) carType = "未知";
                VehicleInfoModel vehicleInfoModel = new VehicleInfoModel(vin, modelCode, modelName, seriesCode, seriesName, salesDate,
                        carType, nickName, liveTime);

                //将数据追加到map对象中
                resultMap.put(vin, vehicleInfoModel);
            }

            //关闭结果集对象
            resultSet.close();

            if(resultMap.isEmpty()){
                logger.warn("从车辆基础信息表中获取的车辆数据是空.");
            }else{
                logger.warn("从车辆基础信息表中获取的车辆数据："+resultMap);
                sourceContext.collect(resultMap);
            }

            TimeUnit.SECONDS.sleep(Long.parseLong(parameterTool.getRequired("vehinfo.millionseconds")));
        }
    }

    /**
     * 取消操作
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
