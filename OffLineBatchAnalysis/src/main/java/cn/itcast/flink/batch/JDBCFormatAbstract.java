package cn.itcast.flink.batch;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * Flink JDBC的操作抽象类
 * 1）批的方式实现数据的读取
 * 2）批的方式实现数据的写入
 *
 */
public abstract class JDBCFormatAbstract {

    /**
     * 1）实现数据的读取
     * @param driverName
     * @param url
     * @param userName
     * @param passWord
     * @param inputSql
     * @param rowTypeInfo
     * @param fetchSize
     * @return
     */
    public JDBCInputFormat getBatchJDBCInputFormat(
            String driverName, String url, String userName, String passWord, String inputSql, RowTypeInfo rowTypeInfo, Integer fetchSize){
        JDBCInputFormat jdbcInputFormat = null;
        JDBCInputFormat.JDBCInputFormatBuilder inputFormatBuilder = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverName)
                .setDBUrl(url)
                .setUsername(userName)
                .setPassword(passWord)
                .setQuery(inputSql)
                .setRowTypeInfo(rowTypeInfo);

        //如果fetchSize不为空，则设置fetchSize
        if(Objects.nonNull(fetchSize)){
            jdbcInputFormat = inputFormatBuilder.setFetchSize(fetchSize).finish();
        }else{
            jdbcInputFormat = inputFormatBuilder.finish();
        }

        //返回JDBCInputFormat对象
        return jdbcInputFormat;
    }

    /**
     * 1）实现数据的读取
     * @param driverName
     * @param url
     * @param userName
     * @param passWord
     * @param inputSql
     * @param rowTypeInfo
     * @return
     */
    public JDBCInputFormat getBatchJDBCInputFormat(
            String driverName, String url, String userName, String passWord, String inputSql, RowTypeInfo rowTypeInfo){
        return  getBatchJDBCInputFormat(driverName, url, userName, passWord, inputSql, rowTypeInfo, null);
    }

    /**
     * 2）批的方式实现数据的写入
     * @param driverName
     * @param url
     * @param userName
     * @param passWord
     * @param outputSql
     * @param sqlType
     * @param batchInterval
     * @return
     */
    public JDBCOutputFormat getBatchJDBCOutputFormat(
            String driverName, String url, String userName, String passWord, String outputSql, int[] sqlType, Integer batchInterval){
        JDBCOutputFormat jdbcOutputFormat = null;
        JDBCOutputFormat.JDBCOutputFormatBuilder outputFormatBuilder = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(driverName)
                .setDBUrl(url)
                .setUsername(userName)
                .setPassword(passWord)
                .setQuery(outputSql);

        //如果这两个参数都不为空，则追加到builder对象
        if(Objects.nonNull(sqlType) && Objects.nonNull(batchInterval)){
            JDBCOutputFormat.JDBCOutputFormatBuilder jdbcFormatBuilder = null;
            jdbcFormatBuilder = outputFormatBuilder.setSqlTypes(sqlType).setBatchInterval(batchInterval);
            jdbcOutputFormat = jdbcFormatBuilder.finish();
        }else{
            JDBCOutputFormat.JDBCOutputFormatBuilder jdbcFormatBuilder = null;
            if(Objects.nonNull(sqlType)){
                jdbcFormatBuilder = outputFormatBuilder.setSqlTypes(sqlType);
            }
            if(Objects.nonNull(batchInterval)){
                jdbcFormatBuilder = outputFormatBuilder.setBatchInterval(batchInterval);
            }
            if(jdbcFormatBuilder == null){
                jdbcFormatBuilder = outputFormatBuilder;
            }
            jdbcOutputFormat = jdbcFormatBuilder.finish();
        }

        return jdbcOutputFormat;
    }

    /**
     * 2）批的方式实现数据的写入
     * @param driverName
     * @param url
     * @param userName
     * @param passWord
     * @param outputSql
     * @return
     */
    public JDBCOutputFormat getBatchJDBCOutputFormat(
            String driverName, String url, String userName, String passWord, String outputSql){
        return getBatchJDBCOutputFormat(driverName, url, userName, passWord, outputSql, null, null);
    }

    /**
     * 2）批的方式实现数据的写入
     * @param driverName
     * @param url
     * @param userName
     * @param passWord
     * @param outputSql
     * @param sqlType
     * @return
     */
    public JDBCOutputFormat getBatchJDBCOutputFormat(
            String driverName, String url, String userName, String passWord, String outputSql, int[] sqlType) {
        return getBatchJDBCOutputFormat(driverName, url, userName, passWord, outputSql, sqlType, null);
    }
}
