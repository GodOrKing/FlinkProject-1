package cn.itcast.batch.test;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * flink加载hive表的数据测试类
 */
public class FlinkHiveBatchTest {
    //定义logger对象实例
    private final static Logger logger = LoggerFactory.getLogger(FlinkHiveBatchTest.class);
    //定义jdbc的连接地址
    private final static String URL = "jdbc:hive2://node03:10000/itcast_ods?characterEncoding=utf8&useSSL=false";
    //定义用户名
    private final static String USER = "root";
    //定义密码
    private final static String PASSWORD = "123456";
    //定义驱动名称
    private final static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    //定义表名
    private final static String TABLE_NAME= "itcast_error";
    //定义sql语句
    private final static String SQL = "select name,age,country from itcast_error";

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        JDBCInputFormat.JDBCInputFormatBuilder inputFormatBuilder = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(DRIVER_NAME)
                .setDBUrl(URL)
                .setUsername(USER)
                .setPassword(PASSWORD);

        Calendar calendar = Calendar.getInstance();
        //获取当前月
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dd = sdf.format(calendar.getTime());

        //定义列名的数组对象
        TypeInformation[] types = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
        String[] colNames = new String[]{"json", "yearmonth"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, colNames);
        JDBCInputFormat jdbcInputFormat = inputFormatBuilder.setQuery(SQL + "where yearmonth='" + dd + "' limit 10")
                .setRowTypeInfo(rowTypeInfo).finish();
        DataSource<Row> dataSource = env.createInput(jdbcInputFormat);

        try {
            dataSource.print();
        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}
