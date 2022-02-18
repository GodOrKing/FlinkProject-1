package cn.itcast.streaming.sink;

import cn.itcast.entity.ItcastDataObj;
import cn.itcast.utils.DateUtil;
import cn.itcast.utils.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义实现车辆明细表数据存储到hbase表中
 * 经常用于分析的字段存储到hbase独立的表中
 */
public class VehicleDetailSinkOptimizer extends RichSinkFunction<ItcastDataObj> {
    //定义日志操作对象
    private final static Logger logger = LoggerFactory.getLogger(VehicleDetailSinkOptimizer.class);

    //定义hbase的表名
    private String tableName;
    //定义connection
    private Connection connection;
    //定义bufferedMutator
    private BufferedMutator bufferedMutator;
    //定义列族的名称
    private String cf = "cf";
    /**
     * 构造方法
     * @param tableName
     */
    public VehicleDetailSinkOptimizer(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 初始化方法，每个线程执行一次
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //1：定义hbase的连接配置对象
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", parameterTool.getRequired("zookeeper.quorum"));
        configuration.set("hbase.zookeeper.property.clientPort", parameterTool.getRequired("zookeeper.clientPort"));
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        //2：创建hbase的连接对象
        connection = ConnectionFactory.createConnection();
        //定义BufferedMutatorParams的参数对象
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
        params.writeBufferSize(128*1024*1024);
        //实例化bufferedMutator对象实例
        bufferedMutator = connection.getBufferedMutator(params);
        logger.warn("获得hbase的连接对象，{}表对象初始化成功", tableName);
    }

    /**
     * 关闭连接，释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if(bufferedMutator!=null) bufferedMutator.close();
        if(connection != null) connection.close();
    }

    /**
     * 每条数据执行一次该方法
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(ItcastDataObj value, Context context) throws Exception {
        //将每条数据写入到hbase中，需要将itcastDataObject转换成Put对象
        Put put = setDataSourcePut(value);

        bufferedMutator.mutate(put);
        //强制刷新数据到hbase中（异步操作）
        bufferedMutator.flush();
    }

    /**
     * 将itcastDataObj转换成Put对象返回
     * @param itcastDataObj
     * @return
     */
    private Put setDataSourcePut(ItcastDataObj itcastDataObj) {
        //确定rowkey
        String rowKey = itcastDataObj.getVin() + StringUtil.reverse(itcastDataObj.getTerminalTimeStamp().toString());
        Put put = new Put(Bytes.toBytes(rowKey));
        //设置需要写入的列有那些
        //这两个列一定不为空，如果为空就不是正常数据了
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vin"), Bytes.toBytes(itcastDataObj.getVin()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("terminalTime"), Bytes.toBytes(itcastDataObj.getTerminalTime()));

        //电量百分比(currentElectricity)、当前电量(remainPower)、百公里油耗(fuelConsumption100km)、
        // 发动机速度(engineSpeed)、车辆速度(vehicleSpeed)
        if(itcastDataObj.getCurrentElectricity() != -999999D){
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("currentElectricity"), Bytes.toBytes(itcastDataObj.getCurrentElectricity()));
        }
        if(itcastDataObj.getRemainPower() != -999999D){
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("remainPower"), Bytes.toBytes(itcastDataObj.getRemainPower()));
        }
        if(StringUtils.isNotEmpty(itcastDataObj.getFuelConsumption100km()) ){
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("fuelConsumption100km"), Bytes.toBytes(itcastDataObj.getFuelConsumption100km()));
        }
        if(StringUtils.isNotEmpty(itcastDataObj.getEngineSpeed()) ){
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("engineSpeed"), Bytes.toBytes(itcastDataObj.getEngineSpeed()));
        }
        if(itcastDataObj.getVehicleSpeed() != -999999D){
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vehicleSpeed"), Bytes.toBytes(itcastDataObj.getVehicleSpeed()));
        }
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("processTime"), Bytes.toBytes(DateUtil.getCurrentDateTime()));

        //返回put对象
        return  put;
    }
}
