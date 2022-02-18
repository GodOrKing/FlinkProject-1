package cn.itcast.streaming.sink;

import cn.itcast.entity.TripModel;
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
 * 驾驶行程数据实时写入到hbase表中
 */
public class TripDriveToHBaseSink extends RichSinkFunction<TripModel> {
    //定义日志操作对象
    private final static Logger logger = LoggerFactory.getLogger(TripDriveToHBaseSink.class);
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
    public TripDriveToHBaseSink(String tableName) {
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
     * @param tripModel
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(TripModel tripModel, Context context) throws Exception {
        Put put = markPut(tripModel);
        bufferedMutator.mutate(put);

        //强制刷新缓冲区，同步到hbase中
        bufferedMutator.flush();
    }

    /**
     * 将tripModel转换成put对象
     * @param tripModel
     * @return
     */
    private Put markPut(TripModel tripModel) {
        String rowKey = tripModel.getVin() + StringUtil.reverse(
                DateUtil.convertStringToDate(tripModel.getTripStartTime()).getTime()+""
        );
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vin"), Bytes.toBytes(tripModel.getVin()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lastSoc"), Bytes.toBytes(String.valueOf(tripModel.getLastSoc())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lastMileage"), Bytes.toBytes(String.valueOf(tripModel.getLastMileage())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tripStartTime"), Bytes.toBytes(tripModel.getTripStartTime()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getStart_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_longitude"), Bytes.toBytes(String.valueOf(tripModel.getStart_longitude())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_latitude"), Bytes.toBytes(String.valueOf(tripModel.getStart_latitude())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_mileage"), Bytes.toBytes(String.valueOf(tripModel.getStart_mileage())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getEnd_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_longitude"), Bytes.toBytes(String.valueOf(tripModel.getEnd_longitude())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_latitude"), Bytes.toBytes(String.valueOf(tripModel.getEnd_latitude())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_mileage"), Bytes.toBytes(String.valueOf(tripModel.getEnd_mileage())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tripEndTime"), Bytes.toBytes(tripModel.getTripEndTime()));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("mileage"), Bytes.toBytes(String.valueOf(tripModel.getMileage())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("max_speed"), Bytes.toBytes(String.valueOf(tripModel.getMax_speed())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("soc_comsuption"), Bytes.toBytes(String.valueOf(tripModel.getSoc_comsuption())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("time_comsuption"), Bytes.toBytes(String.valueOf(tripModel.getTime_comsuption())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("total_low_speed_nums"), Bytes.toBytes(String.valueOf(tripModel.getTotal_low_speed_nums())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("total_medium_speed_nums"), Bytes.toBytes(String.valueOf(tripModel.getTotal_medium_speed_nums())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("total_high_speed_nums"), Bytes.toBytes(String.valueOf(tripModel.getTotal_high_speed_nums())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Low_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getLow_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Medium_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getMedium_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("High_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getHigh_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Low_BMS_Mileage"), Bytes.toBytes(String.valueOf(tripModel.getLow_BMS_Mileage())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Medium_BMS_Mileage"), Bytes.toBytes(String.valueOf(tripModel.getMedium_BMS_Mileage())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("High_BMS_Mileage"), Bytes.toBytes(String.valueOf(tripModel.getHigh_BMS_Mileage())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tripStatus"), Bytes.toBytes(String.valueOf(tripModel.getTripStatus())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("processTime"), Bytes.toBytes(DateUtil.getCurrentDateTime()));
        return put;
    }
}
