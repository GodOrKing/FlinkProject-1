package cn.itcast.flink.batch.task;

import cn.itcast.flink.batch.TotalDataRate;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.types.Row;

/**
 * 按照计算总数据的准确率
 */
public class TotalDateRateTask {
    public static void main(String[] args) {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            //创建核心逻辑实现对象
            TotalDataRate totalDataRate = new TotalDataRate();
            //得到hive表查询的正确数据总数和异常数据总数
            JDBCInputFormat hiveJDBCInputFormat = totalDataRate.getHiveJDBCInputFormat();
            //把hive查询的结果封装为dataSet对象
            DataSource<Row> hiveDataSource = env.createInput(hiveJDBCInputFormat);
            //将查询到的数据DataSource<Row<Long, Long>>转换成DataSource<Row<String, long, long, float, float, String>>
            DataSet<Row> hiveDataSet = totalDataRate.convertHiveDataSource(hiveDataSource);
            //得到jdbcOutFormat
            JDBCOutputFormat mysqlJDBCOutputFormat = totalDataRate.getMysqlJDBCOutputFormat();
            //将hive拉宽后的数据写入到mysql数据表中
            hiveDataSet.output(mysqlJDBCOutputFormat);
            //执行作业
            env.execute();
        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}
