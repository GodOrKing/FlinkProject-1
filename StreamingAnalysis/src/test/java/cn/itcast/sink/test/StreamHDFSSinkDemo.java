package cn.itcast.sink.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 演示StreamingFileSink的使用
 * 自定义数据源生成器不断生成数据，将数据以流的方式实时的写入到hdfs中
 */
public class StreamHDFSSinkDemo {
    public static void main(String[] args) throws Exception {
        /**
         * 实现步骤：
         * 1：创建flink流处理的运行环境
         * 2：构建数据源（自定义数据源）
         * 3：开启checkpoint
         * 4：设置一个并行度写入数据
         * 5：将数据实时写入到hdfs中（指定分桶策略及滚动策略）
         * 6：递交作业执行
         */
        System.setProperty("HADOOP_USER_NAME", "root");
        //TODO 1：创建flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 2：构建数据源（自定义数据源）
        DataStreamSource<Order> streamSource = env.addSource(new MyNoParallelSource());

        streamSource.print();
        //TODO 3：开启checkpoint
        env.enableCheckpointing(5000);

        //TODO 4：设置一个并行度写入数据
        env.setParallelism(1);

        //TODO 5：将数据实时写入到hdfs中（指定分桶策略及滚动策略）
        //指定写入到的路径
        String outputPath = "hdfs://node01:8020/test/streamingfilesink/output";
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();

        StreamingFileSink<Order> streamingFileSink = StreamingFileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder<Order>("utf-8"))
                /**
                 * 指定分桶策略：
                 * DateTimeBucketAssigner:默认的分桶方式，每小时分一个桶，可以指定：yyyy-MM-dd，表示一天一个桶
                 * BasePathBucketAssigner:把所有的文件放到一个基本路径下的分桶器（全局桶）
                 */
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                /**
                 * 指定滚动策略：
                 *CheckpointRollingPolicy
                 * DefaultRollingPolicy
                 * OnCheckpointRollingPolicy
                 */
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(5))//设置滚动时间间隔，每隔5s中产生一个文件
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(2))//设置不活动的时间间隔，2秒钟没有新的数据写入，则滚动桶
                                .withMaxPartSize(1024 * 1024 * 128)//设置文件大小滚动策略，每隔128M滚动一次桶
                                .build()
                ).withOutputFileConfig(config).build();

                streamSource.addSink(streamingFileSink);
        //TODO 6：递交作业执行
        env.execute();
    }

    private static class MyNoParallelSource implements SourceFunction<Order> {
        private boolean isRunning = true;
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while (isRunning){
                Random random = new Random();
                String orderId = UUID.randomUUID().toString();
                String userId = String.valueOf(random.nextInt(3));
                int money = random.nextInt(100);
                long timestamp = System.currentTimeMillis();

                Order order = new Order(orderId, userId, money, timestamp);
                sourceContext.collect(order);

                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class  Order {
        private String id;
        private String userId;
        private int money;
        private  Long timestamp;
    }
}
