package cn.itcast.streaming.task;

import cn.itcast.entity.ItcastDataObj;
import cn.itcast.streaming.sink.SrcDataToHBaseSink;
import cn.itcast.streaming.sink.SrcDataToHBaseSinkOptimizer;
import cn.itcast.streaming.sink.VehicleDetailSinkOptimizer;
import cn.itcast.utils.JsonParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * 原始数据实时ETL开发
 * flink实时消费kafka数据，将消费出来的数据转换、清洗、加工、基础数据加载（可选）、数据逻辑处理，流式写入数据存储介质等等操作
 * 1）正常数据需要存储到hdfs和hbase中
 * 2）异常数据需要存储到hdfs中
 */
public class KafkaSourceDataTask extends BaseTask {
    /**
     * 入口方法
     * @param args
     */
    public static void main(String[] args) throws Exception {
        /***
         * 实现步骤：
         * 1）初始化flink流式处理的开发环境
         * 2）按照事件时间处理数据（terminalTimeStamp）进行窗口的划分和水印的添加
         * 3）开启checkpoint
         *  3.1：设置每隔30s周期性开启checkpoint
         *  3.2：设置检查点的model、exactly-once、保证数据一次性语义
         *  3.3：设置两次checkpoint的时间间隔，避免两次间隔太近导致频繁的checkpoint，而出现业务处理能力下降
         *  3.4：设置checkpoint的超时时间
         *  3.5：设置checkpoint的最大尝试次数，同一个时间有几个checkpoint在运行
         *  3.6：设置checkpoint取消的时候，是否保留checkpoint，checkpoint默认会在job取消的时候删除checkpoint
         *  3.7：设置执行job过程中，保存检查点错误时，job不失败
         *  3.8：设置检查点的存储位置，使用rocketDBStateBackend，存储本地+hdfs分布式文件系统，可以进行增量检查点
         * 4）设置任务的重启策略（固定延迟重启策略、失败率重启策略、无重启策略）
         *  4.1：如果开启了checkpoint，默认不停的重启，没有开启checkpoint，无重启策略
         * 5）创建flink消费kafka数据的对象，指定kafka的参数信息
         *  5.1：设置kafka的集群地址
         *  5.2：设置消费者组id
         *  5.3：设置kafka的分区感知（动态感知）
         *  5.4：设置key和value的反序列化（可选）
         *  5.5：设置是否自动递交offset
         *  5.6：创建kafka的消费者实例
         *  5.7：设置自动递交offset保存到检查点
         * 6）将kafka消费者对象添加到环境中
         * 7）将json字符串转换成javaBean对象
         * 8）过滤出来异常的数据
         * 9）过滤出来正常的数据
         * 10）将异常的数据写入到hdfs（BucketingSink、StreamingFileSink）
         *  StreamingFileSink是flink1.10的新特性，而在flink1.10之前的版本是没有的，之前使用BucketingSink实现数据流的方式写入到hdfs中
         * 11）将正常的数据写入到hdfs中（StreamingFileSink）
         * 12）将正常的数据写入到hbase中
         * 13）启动作业，运行任务
         */
        //TODO  1）初始化flink流式处理的开发环境
        StreamExecutionEnvironment env = getEnv(KafkaSourceDataTask.class.getSimpleName());

        //TODO 6）将kafka消费者对象添加到环境中
        DataStream<String> kafkaStreamSource = createKafkaStream(SimpleStringSchema.class);
        kafkaStreamSource.printToErr("原始数据>>>");

        //TODO  7）将json字符串转换成javaBean对象
        SingleOutputStreamOperator<ItcastDataObj> itcastDataObjStream = kafkaStreamSource.filter(obj-> StringUtils.isNotEmpty(obj)).map(JsonParseUtil::parseJsonToObject);
        itcastDataObjStream.print("解析后的javaBean对象>>>");

        //TODO  8）过滤出来异常的数据
        SingleOutputStreamOperator<ItcastDataObj> errorDataStream = itcastDataObjStream.filter(itcastDataObj -> StringUtils.isNotEmpty(itcastDataObj.getErrorData()));
        errorDataStream.print("异常数据>>>");
        //TODO  9）过滤出来正常的数据
        SingleOutputStreamOperator<ItcastDataObj> srcDataStream = itcastDataObjStream.filter(itcastDataObj -> StringUtils.isEmpty(itcastDataObj.getErrorData()));
        srcDataStream.print("正常数据>>>");
        //TODO  10）将异常的数据写入到hdfs（BucketingSink、StreamingFileSink）
        // StreamingFileSink是flink1.10的新特性，而在flink1.10之前的版本是没有的，之前使用BucketingSink实现数据流的方式写入到hdfs中
        //指定写入的文件名称和格式
        OutputFileConfig outputFileConfig = OutputFileConfig.builder().withPartPrefix("prefix").withPartSuffix(".ext").build();
        //实时写入到hdfs的数据最终需要跟hive进行整合，因此可以将数据直接写入到hive默认的数仓路径下
        StreamingFileSink errorFileSink = StreamingFileSink.forRowFormat(
                new Path(parameterTool.getRequired("hdfsUri") + "/apps/hive/warehouse/ods.db/itcast_error"),
                new SimpleStringEncoder<>("utf-8"))
                .withBucketAssigner(new DateTimeBucketAssigner("yyyyMMdd"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(5)) //设置滚动时间间隔，5s滚动一次
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(2)) //设置不活动的时间间隔，未写入数据处于不活动状态的时间时滚动文件
                                .withMaxPartSize(128 * 1024 * 1024) //128M滚动一次文件
                                .build()
        ).withOutputFileConfig(outputFileConfig).build();
        //将itcastDataObject转换成hive所能够解析的字符串
        errorDataStream.map(ItcastDataObj::toHiveString).addSink(errorFileSink);

        //TODO  11）将正常的数据写入到hdfs中（StreamingFileSink）
        StreamingFileSink srcFileSink = StreamingFileSink.forRowFormat(
                new Path(parameterTool.getRequired("hdfsUri") + "/apps/hive/warehouse/ods.db/itcast_src"),
                new SimpleStringEncoder<>("utf-8"))
                .withBucketAssigner(new DateTimeBucketAssigner("yyyyMMdd"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(5)) //设置滚动时间间隔，5s滚动一次
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(2)) //设置不活动的时间间隔，未写入数据处于不活动状态的时间时滚动文件
                                .withMaxPartSize(128 * 1024 * 1024) //128M滚动一次文件
                                .build()
                ).withOutputFileConfig(outputFileConfig).build();
        srcDataStream.map(ItcastDataObj::toHiveString).addSink(srcFileSink);

        //TODO  12）将正常的数据写入到hbase中
        SrcDataToHBaseSink srcDataToHBaseSink = new SrcDataToHBaseSink("itcast_src");
        SrcDataToHBaseSinkOptimizer srcDataToHBaseSinkOptimizer = new SrcDataToHBaseSinkOptimizer("itcast_src");
        srcDataStream.addSink(srcDataToHBaseSinkOptimizer);

        //TODO 13）将经常用于分析的字段提取出来保存到一个hbase独立的表中，这个表的字段要远远小于正常数据宽表的字段数量，
        // 将来与Phoenix整合以后，在可视化页面zepplin中分析查询hbase表数据
        VehicleDetailSinkOptimizer vehicleDetailSinkFunction = new VehicleDetailSinkOptimizer("itcastsrc_vehicle_detail");
        srcDataStream.addSink(vehicleDetailSinkFunction);

        //TODO  14）启动作业，运行任务
        env.execute();
    }
}
