package cn.itcast.streaming.task;

import cn.itcast.utils.ConfigLoader;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * 定义所有task作业的父类，在父类中实现公共的代码
 * 加载配置文件内容到ParameterTool对象中
 * 1）flink流处理环境的初始化
 * 2）flink接入kafka数据源消费数据
 */
public abstract class BaseTask {
    private static Logger logger = LoggerFactory.getLogger(BaseTask.class);

    //定义parameterTool工具类
    public static ParameterTool parameterTool;
    public static String appName;

    /**
     * 定义静态代码块，加载配置文件数据到ParameterTool对象中
     */
    static {
        try {
            parameterTool = ParameterTool.fromPropertiesFile(BaseTask.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //todo 1）初始化flink流式处理的开发环境
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * TODO 1）flink任务的初始化方法
     * @return
     */
    public static StreamExecutionEnvironment getEnv(String className){
        System.setProperty("HADOOP_USER_NAME", "root");
        //设置全局的参数（使用的时候可以直接用法：getRuntimeContext()）
        env.getConfig().setGlobalJobParameters(parameterTool);
        //todo  2）按照事件时间处理数据（terminalTimeStamp）进行窗口的划分和水印的添加
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //为了后续进行测试方便，将并行度设置为1，在生产环境一定不要设置代码级别的并行度，可以设置client级别的并行度
        env.setParallelism(1);

        //todo 3）开启checkpoint
        //  3.1：设置每隔30s周期性开启checkpoint
        env.enableCheckpointing(30*1000);
        //  3.2：设置检查点的model、exactly-once、保证数据一次性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //  3.3：设置两次checkpoint的时间间隔，避免两次间隔太近导致频繁的checkpoint，而出现业务处理能力下降
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(20 * 1000);
        //  3.4：设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(20*1000);
        //  3.5：设置checkpoint的最大尝试次数，同一个时间有几个checkpoint在运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //  3.6：设置checkpoint取消的时候，是否保留checkpoint，checkpoint默认会在job取消的时候删除checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //  3.7：设置执行job过程中，保存检查点错误时，job不失败
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        //  3.8：设置检查点的存储位置，使用rocketDBStateBackend，存储本地+hdfs分布式文件系统，可以进行增量检查点
        String hdfsBasePath = parameterTool.getRequired("hdfsUri");
        try {
            env.setStateBackend(new RocksDBStateBackend(hdfsBasePath+"/flink/checkpoint/"+ className));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //todo  4）设置任务的重启策略（固定延迟重启策略、失败率重启策略、无重启策略）
        //todo  4.1：如果开启了checkpoint，默认不停的重启，没有开启checkpoint，无重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());

        appName = className;

        //返回env对象
        return env;
    }

    /**
     * TODO 2）flink接入kafka数据源消费数据
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> DataStream<T> createKafkaStream(Class<? extends DeserializationSchema> clazz)  {
        DataStreamSource<T> streamSource = null;
        try {
            //todo  5）创建flink消费kafka数据的对象，指定kafka的参数信息
            Properties props = new Properties();
            // 5.1：设置kafka的集群地址
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.getRequired("bootstrap.servers"));
            // 5.2：设置消费者组id
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, parameterTool.getRequired("kafka.group.id")+appName);
            // 5.3：设置kafka的分区感知（动态感知）(在kafka0.11版本中可以不指定动态感知功能，因为在kafka0.11中有粘性分区)
            props.setProperty("flink.partition-discovery.interval-millis", "30000");
            // 5.4：设置key和value的反序列化（可选）
            // 5.5：设置是否自动递交offset
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, parameterTool.get("enable.auto.reset", "earliest"));
            // 5.6：创建kafka的消费者实例
            FlinkKafkaConsumer011<T> kafkaConsumer011= new FlinkKafkaConsumer011<T>(
                    parameterTool.getRequired("kafka.topic"),
                    clazz.newInstance(),
                    props
            );
            // 5.7：设置自动递交offset保存到检查点
            kafkaConsumer011.setCommitOffsetsOnCheckpoints(true);
            //todo  6）将kafka消费者对象添加到环境中
            streamSource = env.addSource(kafkaConsumer011);

        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        //返回消费到的数据
        return streamSource;
    }
}
