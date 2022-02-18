package cn.itcast.streaming.sink;

import cn.itcast.utils.DateUtil;
import cn.itcast.utils.StringUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将驾驶行程采样数据写入到hbase中，作为采样分析的数据源
 */
public class TripSampleToHBaseSink extends RichSinkFunction<String[]> {
    //定义日志操作对象
    private final static Logger logger = LoggerFactory.getLogger(TripSampleToHBaseSink.class);
    //定义操作的hbase的表明
    private String tableName;
    //定义connection连接对象
    private Connection connection;
    //定义BufferedMutator
    private BufferedMutator bufferedMutator;
    //定义列族的名称
    private String cf = "cf";

    /**
     * 定义构造方法
     * @param tableName
     */
    public TripSampleToHBaseSink(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 初始化方法，只被执行一次
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取到全局参数对象
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //1：定义hbase的连接配置对象
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", parameterTool.getRequired("zookeeper.quorum"));
        configuration.set("hbase.zookeeper.property.clientPort", parameterTool.getRequired("zookeeper.clientPort"));
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        //2：创建hbase的连接对象
        connection = ConnectionFactory.createConnection();
        //实例化table对象
        //table = connection.getTable(TableName.valueOf(tableName));
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
        bufferedMutator = connection.getBufferedMutator(params);
        logger.warn("获取hbase的连接对象，{}表对象初始化成功.", tableName);
        System.out.println("表初始化成功");
    }

    /**
     * 关闭连接，释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if(bufferedMutator != null) bufferedMutator.close();
        if(connection !=null && !connection.isClosed()) connection.close();
    }

    /**
     * 将数据一条条的写入到hbase中
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(String[] value, Context context) throws Exception {
        Put put = markPut(value);
        bufferedMutator.mutate(put);

        //强制刷新缓冲区，同步到hbase中
        bufferedMutator.flush();
    }

    /**
     * 根据数组对象生成Put对象
     * @param value
     * @return
     */
    private Put markPut(String[] value) {
        //生成rowkey
        String rowKey = value[0]+ StringUtil.reverse(value[1]);
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("soc"), Bytes.toBytes(value[2]));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("meter"), Bytes.toBytes(value[3]));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("speed"), Bytes.toBytes(value[4]));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("gps"), Bytes.toBytes(value[5]));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("terminalTime"), Bytes.toBytes(value[6]));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("processTime"), Bytes.toBytes(DateUtil.getCurrentDateTime()));

        //返回put
        return put;
    }
}
