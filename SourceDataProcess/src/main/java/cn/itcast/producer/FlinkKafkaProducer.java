package cn.itcast.producer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * kafka生产者实例
 * 读取sourcedata.txt文件数据，批的方式写入到kafka集群
 */
public class FlinkKafkaProducer {
    public static void main(String[] args) throws Exception {
        /**
         * 实现步骤：
         * 1：创建flink流式程序的开发环境
         * 2：设置应用程序按照事件时间处理数据
         * 3：加载原始数据目录中的文件（sourcedata.txt），返回dataStream对象
         * 4：创建kafka的生产者实例，将数据写入到kafka集群
         * 5：启动运行
         */

        //TODO 1：创建flink流式程序的开发环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 2：设置应用程序按照事件时间处理数据
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //TODO 3：加载原始数据目录中的文件（sourcedata.txt），返回dataStream对象
        DataStreamSource<String> lines = env.readTextFile("F:\\深圳黑马云计算大数据就业21期（20201024面授）\\星途车联网大数据\\全部讲义\\1-星途车联网系统第一章-项目基石与前瞻\\原始数据");

        //TODO 4：创建kafka的生产者实例，将数据写入到kafka集群
        FlinkKafkaProducer011 kafkaProducer011 = new FlinkKafkaProducer011(
                "node01:9092,node02:9092,node03:9092",
                "vehiclejsondata",
                new SimpleStringSchema()
        );
        lines.addSink(kafkaProducer011);

        //TODO 5：启动运行
        env.execute();
    }
}
