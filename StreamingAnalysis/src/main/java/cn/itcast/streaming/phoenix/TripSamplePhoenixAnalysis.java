package cn.itcast.streaming.phoenix;

import cn.itcast.utils.DateUtil;
import cn.itcast.utils.JDBCUtil;
import cn.itcast.utils.PhoenixJDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TripSamplePhoenixAnalysis {
    private static Logger logger = LoggerFactory.getLogger(TripSamplePhoenixAnalysis.class);

    public static void main(String[] args) {
        try {
//            createSchema();
//            createTripSampleView();
            tripSampleTotalNum();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @desc 创建Schema
     * @throws SQLException
     */
    private static void createSchema() throws SQLException {
        String createSql = "CREATE SCHEMA IF NOT EXISTS TRIPDB";
        PhoenixJDBCUtil.createSchema(createSql);
        logger.warn("创建SCHEMA成功,sql:{}", createSql);
    }

    /**
     * @desc 创建驾驶行程采样表
     * @throws SQLException
     */
    private static void createTripSampleView() throws SQLException {
        String createSql = "CREATE VIEW TRIPDB.\"trip_sample\" (\"rowNum\" varchar PRIMARY KEY, \"cf\".\"soc\" varchar, \"cf\".\"mileage\" varchar, \"cf\".\"speed\" varchar, \"cf\".\"gps\" varchar, \"cf\".\"terminalTime\" varchar, \"cf\".\"processTime\" varchar)";
        PhoenixJDBCUtil.create(createSql);
        logger.warn("创建行程采样phoenix视图成功，sql：{}", createSql);
    }

    /**
     * @desc 行程采样地域分析
     * @throws SQLException
     */
    private static void tripSampleTotalNum() throws SQLException {
        String sql = " select count(1) from TRIPDB.\"trip_sample\"";
        List<String[]> resultList = PhoenixJDBCUtil.select(sql);
        long totalNum = 0;
        // 第一层循环遍历有多条数据的记录
        for (String[] strings : resultList) {
            // 第二层循环遍历多个属性
            for (String str : strings) {
                totalNum = Long.parseLong(str);
            }
        }
        System.out.println("总样本数：" + totalNum);
        String insertSql = "insert into vehicle_networking.t_sample_result(name, totalNum, processTime) values (?,?,?)";
        ArrayList<Object> arrayList = new ArrayList<>();
        arrayList.add(0, "采样样本总数");
        arrayList.add(1, totalNum);
        arrayList.add(2, DateUtil.getCurrentDateTime());
        JDBCUtil.executeInsert(insertSql, arrayList);
        logger.warn("插入数据到样本结果表中成功,sql:{}", insertSql);
    }

}