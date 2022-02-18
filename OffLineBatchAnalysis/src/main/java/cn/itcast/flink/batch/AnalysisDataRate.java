package cn.itcast.flink.batch;

import cn.itcast.flink.batch.utils.ConfigLoader;
import cn.itcast.flink.batch.utils.DateUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.Objects;
import java.util.UUID;

/**
 * 实现多维分析数据的准确率（正确数据、异常数据）
 */
public class AnalysisDataRate extends JDBCFormatAbstract {
    private Logger logger = LoggerFactory.getLogger(AnalysisDataRate.class);
    //计算指定天的数据准确率
    private final  static  String day = DateUtil.getTodayDate();
    //计算指定周的数据准确率
    private final static String week = DateUtil.getNowWeekStart();
    //计算指定月的数据准确率
    private final static String month = DateUtil.getYearMonthDate() + "00";
    //指定统计条件
    private String condition;

    /**
     * 构造方法，接受传递的条件
     * @param condition
     */
    public AnalysisDataRate(String condition){
        this.condition = condition;
    }

    /**
     * 执行任务的主方法
     */
    public void executeTask(){
        //获取运行flink作业的批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //验证condition是否是空的
        Objects.requireNonNull(condition, "执行计算任务的时候必须要传递条件...");

        AnalysisDataRate dataRate = new AnalysisDataRate(condition);
        //按照维度获取到数据，得到jdbcInputFormat
        JDBCInputFormat hiveJdbcInputFormat = dataRate.getHiveJdbcInputFormat();
        //根据inputformat对象，获取到数据
        DataSet<Row> hiveDataSet = env.createInput(hiveJdbcInputFormat);
        //'记录序列号',
        // '原数据正确数据总数',
        // '原数据错误数据总数',
        // '原始数据正确率',
        // '原始数据错误率',
        // '按天统计',
        // '记录计算时间'
        //将获取到的DataSet<Row<Long, Long>>转换成DataSet<Row<String, Long, Long, Float, Float, String, String>>
        DataSet<Row> rowDataSet = convertHiveDateset(hiveDataSet);
        //将转换后的结果输出到mysql数据库中
        JDBCOutputFormat mysqlJdbcOutputFormat = dataRate.getMysqlJdbcOutputFormat();
        rowDataSet.output(mysqlJdbcOutputFormat);

        //动态指定任务名称
        String[] taskNameArray = new String[]{"TotalDayRateTask", "TotalWeekRateTask", "TotalMonthRateTask"};
        String taskName = setDynamicValue(condition, taskNameArray);

        try {
            env.execute(taskName);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    /**
     * 返回mysql的输出jdbc对象
     * @return
     */
    protected JDBCOutputFormat getMysqlJdbcOutputFormat(){
        //驱动名称
        String driverName = ConfigLoader.getProperty("jdbc.driver");
        //地址
        String url = ConfigLoader.getProperty("jdbc.url");
        //用户名
        String userName = ConfigLoader.getProperty("jdbc.user");
        //密码
        String passWord =  ConfigLoader.getProperty("jdbc.password");
        //定义表的集合数组
        String[] mysqlTable = new String[]{"itcast_data_rate_day", "itcast_data_rate_week", "itcast_data_rate_month"};
        //表名
        String mysqlTableDataRateName =setDynamicValue(condition, mysqlTable);

        //定义字段类型
        int[] sqlTypes = new int[]{ Types.VARCHAR, Types.BIGINT, Types.BIGINT, Types.FLOAT, Types.FLOAT, Types.VARCHAR, Types.VARCHAR };

        //定义插入的sql语句
        String sql = "insert into "+mysqlTableDataRateName+"(series_no, src_total_num,error_total_num,data_accuracy,data_error_rate,day,process_date)" +
                "values(?,?,?,?,?,?,?)";

        return getBatchJDBCOutputFormat(driverName, url, userName, passWord, sql, sqlTypes);
    }

    /**
     * 根据统计条件返回操作的表名
     * @param condition
     * @return
     */
    private String setDynamicValue(String condition, String[] mysqlTables) {
        if(condition.equals(day)){
            return mysqlTables[0];
        }else if (condition.equals(week)){
            return mysqlTables[1];
        }else if(condition.equals(month)){
            return mysqlTables[2];
        }else{
            return mysqlTables[0];
        }
    }

    /**
     * 转换hive查询的结果为最终的计算结果
     * @param hiveDataSet
     */
    private DataSet<Row> convertHiveDateset(DataSet<Row> hiveDataSet) {
        return hiveDataSet.map(row -> {
            long srcTotalNum = Long.parseLong( row.getField(0).toString());
            long errorTotalNum = Long.parseLong( row.getField(1).toString());
            //原始数据正确率
            long dataAccuracy = srcTotalNum / (srcTotalNum + errorTotalNum);
            Row resultRow = new Row(7);
            resultRow.setField(0, UUID.randomUUID().toString());
            resultRow.setField(1, srcTotalNum);
            resultRow.setField(2, errorTotalNum);
            resultRow.setField(3, dataAccuracy);
            resultRow.setField(4, 1 - dataAccuracy);
            resultRow.setField(5, condition);
            resultRow.setField(6, DateUtil.getTodayDate());

            return resultRow;
        });
    }

    /**
     * 从hive中读取数据
     */
    protected JDBCInputFormat getHiveJdbcInputFormat(){
        //驱动名称
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        //地址
        String url = "jdbc:hive2://node03:10000/itcast_ods";
        //用户名
        String userName = "root";
        //密码
        String passWord = "123456";
        //表名
        String hiveTableSrc = "itcast_src";
        String hiveTableError = "itcast_error";

        //定义sql语句
        String inputSQL = "select srcTotalNum,errorTotalNum from " +
                "(select count(1) srcTotalNum from " + hiveTableSrc + " where dt>="+condition+") src," +
                "(select count(1) errorTotalNum from " + hiveTableError + " where dt>="+condition+ ") error";

        TypeInformation[] types = new TypeInformation[]{BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO};
        String[] colNames = new String[] {"srcTotalNum", "errorTotalNum"};
        //定义rowInfo
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, colNames);

        //调用父类方法
        return getBatchJDBCInputFormat(driverName, url, userName, passWord, inputSQL, rowTypeInfo);
    }
}
