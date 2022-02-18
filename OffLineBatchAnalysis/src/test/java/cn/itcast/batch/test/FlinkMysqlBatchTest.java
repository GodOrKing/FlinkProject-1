package cn.itcast.batch.test;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.stylesheets.LinkStyle;

import java.util.ArrayList;
import java.util.List;

/**
 * flink加载mysql数据测试类
 */
public class FlinkMysqlBatchTest {
    //定义logger对象实例
    private final static Logger  logger = LoggerFactory.getLogger(FlinkMysqlBatchTest.class);
    //定义jdbc的连接地址
    private final static String URL = "jdbc:mysql://node03:3306/test?characterEncoding=utf8&useSSL=false";
    //定义用户名
    private final static String USER = "root";
    //定义密码
    private final static String PASSWORD = "123456";
    //定义驱动名称
    private final static String DRIVER_NAME = "com.mysql.jdbc.Driver";
    //定义sql语句
    private final static String SQL = "select name,age,country from user_info";

    public static void main(String[] args) {
        //初始化运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        try {
//            DataSet<Row> jdbcRead = jdbcRead(env);
//            jdbcRead.print();
            jdbcWrite(env);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    /**
     * 使用JDBCInputFormat读取数据
     * @param env
     * @return
     */
    private static DataSource<Row>  jdbcRead(ExecutionEnvironment env){
        DataSource<Row> inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                //数据库连接驱动名称
                .setDrivername(DRIVER_NAME)
                //数据库连接地址
                .setDBUrl(URL)
                //用户名
                .setUsername(USER)
                //密码
                .setPassword(PASSWORD)
                //sql语句
                .setQuery(SQL)
                //rowTypeInfo，指定接受到的字段类型，必须要跟sql语句的查询字段顺序保持一致
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                //查询批次的数据量
                .setFetchSize(10)
                .finish()
        );
        logger.warn("读取mysql数据成功...");
        return inputMysql;
    }

    /**
     * 使用JDBCOutputFormat写入数据
     * @param env
     */
    private static void jdbcWrite(ExecutionEnvironment env){
        //准备需要写入的数据
        List<Row> rowList = new ArrayList<>();
        //准备第一条数据
        Row row1 = new Row(3);
        row1.setField(0, "张三");
        row1.setField(1, 30);
        row1.setField(2, "中国");

        Row row2 = new Row(3);
        row2.setField(0, "李四");
        row2.setField(1, 40);
        row2.setField(2, "中国");

        Row row3 = new Row(3);
        row3.setField(0, "王五");
        row3.setField(1, 50);
        row3.setField(2, "中国");

        //将数据追加到集合对象中
        rowList.add(row1);
        rowList.add(row2);
        rowList.add(row3);

        //根据本地集合生成dataSource对象
        DataSource<Row> dataSource = env.fromCollection(rowList);
        //将数据写入到mysql
        dataSource.output(JDBCOutputFormat.buildJDBCOutputFormat()
                //数据库连接驱动名称
                .setDrivername(DRIVER_NAME)
                //数据库连接地址
                .setDBUrl(URL)
                //用户名
                .setUsername(USER)
                //密码
                .setPassword(PASSWORD)
                .setBatchInterval(10)
                .setQuery("insert into user_info(name, age, country) values(?,?,?)")
                .finish()
        );

        try {
            env.execute();
            logger.warn("写入mysql数据成功...");
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
