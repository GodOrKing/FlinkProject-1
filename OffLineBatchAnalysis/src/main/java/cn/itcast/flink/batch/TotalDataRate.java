package cn.itcast.flink.batch;

import cn.itcast.flink.batch.utils.ConfigLoader;
import cn.itcast.flink.batch.utils.DateUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.UUID;

/**
 * 计算数据准确率
 * 是一个总的正确数据和异常数据的计算类，每个维度都可以初始化这个实例获取到计算结果
 */
public class TotalDataRate extends JDBCFormatAbstract {
    //实例化logger实例
    private final Logger logger = LoggerFactory.getLogger(TotalDataRate.class);

    /**
     * 通过flink jdbc读取hive中的数据
     * @return
     */
    public JDBCInputFormat getHiveJDBCInputFormat() {
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
                "(select count(1) srcTotalNum from " + hiveTableSrc + ") src," +
                "(select count(1) errorTotalNum from " + hiveTableError + ") error";

        TypeInformation[] types = new TypeInformation[]{BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO};
        String[] colNames = new String[] {"srcTotalNum", "errorTotalNum"};
        //定义rowInfo
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, colNames);

        //调用父类方法
        return getBatchJDBCInputFormat(driverName, url, userName, passWord, inputSQL, rowTypeInfo);
    }

    /**
     * 通过flink jdbc写入mysql中数据
     * @return
     */
    public JDBCOutputFormat getMysqlJDBCOutputFormat(){
        //驱动名称
        String driverName = ConfigLoader.getProperty("jdbc.driver");
        //地址
        String url = ConfigLoader.getProperty("jdbc.url");
        //用户名
        String userName = ConfigLoader.getProperty("jdbc.user");
        //密码
        String passWord =  ConfigLoader.getProperty("jdbc.password");
        //表名
        String mysqlTableDataRateName = "itcast_data_rate_day";

        //定义字段类型
        int[] sqlTypes = new int[]{ Types.VARCHAR, Types.BIGINT, Types.BIGINT, Types.FLOAT, Types.FLOAT, Types.VARCHAR };

        //定义插入的sql语句
        String sql = "insert into "+mysqlTableDataRateName+"(series_no, src_total_num,error_total_num,data_accuracy,data_error_rate,process_date)" +
                "values(?,?,?,?,?,?)";

        return getBatchJDBCOutputFormat(driverName, url, userName, passWord, sql, sqlTypes);
    }


    /**
     * 将hive读取到的列（2个列）转换成最终的计算结果（6个列）
     * @param hiveDataSource
     * @return
     */
    public DataSet<Row> convertHiveDataSource(DataSet<Row> hiveDataSource){
        return hiveDataSource.map(row -> {
            //获取到正确的数据量
            long srcTotalNum = Long.parseLong(row.getField(0).toString());
            //获取到错误的数据量
            long errorTotalNum = Long.parseLong(row.getField(1).toString());
            //data_accuracy：'原始数据正确率'
            long dataAccuracy = srcTotalNum / (srcTotalNum + errorTotalNum);

            //定义返回的Row对象
            Row resultRow = new Row(6);
            resultRow.setField(0, UUID.randomUUID().toString());
            resultRow.setField(1, srcTotalNum);
            resultRow.setField(2, errorTotalNum);
            resultRow.setField(3, dataAccuracy);
            resultRow.setField(4, 1 - dataAccuracy);
            resultRow.setField(5, DateUtil.getTodayDate());

            return resultRow;
        });
    }
}
