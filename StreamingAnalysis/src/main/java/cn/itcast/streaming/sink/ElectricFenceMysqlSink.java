package cn.itcast.streaming.sink;

import cn.itcast.entity.ElectricFenceModel;
import cn.itcast.utils.DateUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 将电子围栏分析结果数据写入到mysql数据中
 */
public class ElectricFenceMysqlSink extends RichSinkFunction<ElectricFenceModel> {
    Logger logger = LoggerFactory.getLogger(ElectricFenceMysqlSink.class);
    Connection conn  = null;
    PreparedStatement pstmt = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool globalJobParameters = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String url = globalJobParameters.getRequired("jdbc.url");
        String user = globalJobParameters.getRequired("jdbc.user");
        String pwd = globalJobParameters.getRequired("jdbc.password");
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(url, user, pwd);
    }

    @Override
    public void invoke(ElectricFenceModel value, Context context) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            //出围栏(且能获取到进围栏状态的)则修改进围栏的状态
            if (value.getStatusAlarm() == 0 && value.getInMysql()) {
                String executeSql = "update vehicle_networking.electric_fence set outTime=?,gpsTime=?,lat=?,lng=?,terminalTime=?,processTime=? where id=?";
                pstmt = conn.prepareStatement(executeSql);
                pstmt.setObject(1, value.getOutEleTime());
                pstmt.setObject(2, value.getGpsTime());
                pstmt.setDouble(3, value.getLat());
                pstmt.setDouble(4, value.getLng());
                pstmt.setObject(5, value.getTerminalTime());
                pstmt.setObject(6, sdf.format(new Date()));
                pstmt.setLong(7, value.getUuid());
            } else {
                // 进入围栏，转换ElectricFenceModel对象，插入结构数据到电子围栏结果表
                String executeSql = "insert into vehicle_networking.electric_fence(vin,inTime,outTime,gpsTime,lat,lng,eleId,eleName,address,latitude,longitude,radius,terminalTime,processTime) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                pstmt = conn.prepareStatement(executeSql);
                pstmt.setString(1, value.getVin());
                pstmt.setObject(2, value.getInEleTime());
                pstmt.setObject(3, value.getOutEleTime());
                pstmt.setObject(4, value.getGpsTime());
                pstmt.setDouble(5, value.getLat());
                pstmt.setDouble(6, value.getLng());
                pstmt.setInt(7, value.getEleId());
                pstmt.setString(8, value.getEleName());
                pstmt.setString(9, value.getAddress());
                pstmt.setDouble(10, value.getLatitude());
                pstmt.setDouble(11, value.getLongitude());
                pstmt.setFloat(12, value.getRadius());
                pstmt.setObject(13, value.getTerminalTime());
                pstmt.setObject(14, DateUtil.getCurrentDate());
            }
            pstmt.execute();
            logger.warn("MysqlSink，批量插入数据成功，插入{}条数据", pstmt.getMaxRows());
        } catch (SQLException e){
            e.printStackTrace();
        } finally {
            if (pstmt != null) pstmt.close();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
