package cn.itcast.streaming.task;

import cn.itcast.entity.ItcastDataObj;
import cn.itcast.entity.TripModel;
import cn.itcast.streaming.function.window.DriveSampleWindowFunction;
import cn.itcast.streaming.function.window.DriveTripWindowFunction;
import cn.itcast.streaming.sink.TripDriveToHBaseSink;
import cn.itcast.streaming.sink.TripSampleToHBaseSink;
import cn.itcast.streaming.watermark.TripDriveWatermark;
import cn.itcast.utils.JsonParseUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 驾驶行程业务开发
 * 1）消费kafka数据过滤出来以后进行应用窗口水印等操作，将驾驶行程相关的数据以及驾驶行程采样数据写入到hbase中
 * 2）将数据写入到hive（自己实现）
 */
public class TripDriveTask extends  BaseTask {
    /**
     * 入口方法
     * @param args
     */
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        /**
         * 实现步骤：
         * 1：初始化flink流处理的运行环境（checkpoint设置、事件时间处理数据、并行度等等）
         * 2：接入kafka数据源将数据读取到返回（指定topic、集群地址、消费者id等等）
         * 3：将消费到的json字符串转换成itcastDataObj
         * 4：过滤出来驾驶行程相关的数据
         * 5：对驾驶行程数据应用水印（允许数据延迟30s）
         * 6：对加了水印的数据进行分组操作，应用窗口操作（session窗口）
         *  6.1：车辆在驾驶行程中如果超过15m没有上报数据，可以认为是上一个行程的结束
         * 7：驾驶行程采样数据的业务开发
         *  7.1：对数据应用自定义窗口的业务逻辑处理（划分四类数据：5s、10s、15s、20s）
         *  7.2：将自定义驾驶行程采样业务处理后的数据写入到hbase中
         * 8：驾驶行程数据的业务开发
         *  8.1：驾驶行程数据应用自定义窗口的业务逻辑处理
         *  8.2：将自定义驾驶行程业务处理后的数据写入到hbase中
         * 9：递交作业
         */

        //TODO 1：初始化flink流处理的运行环境（checkpoint设置、事件时间处理数据、并行度等等）
        StreamExecutionEnvironment env = getEnv(TripDriveTask.class.getSimpleName());

        //TODO 2：接入kafka数据源将数据读取到返回（指定topic、集群地址、消费者id等等）
        DataStream<String> kafkaStream = createKafkaStream(SimpleStringSchema.class);
        kafkaStream.printToErr("原始数据>>>");

        //TODO 3：将消费到的json字符串转换成itcastDataObj
        SingleOutputStreamOperator<ItcastDataObj> itcastDataObjStream = kafkaStream.map(JsonParseUtil::parseJsonToObject);

        //TODO 4：过滤出来驾驶行程相关的数据(充电状态为2或者3的数据为驾驶行程数据)
        SingleOutputStreamOperator<ItcastDataObj> tripDriveDataStream = itcastDataObjStream.filter(
                itcastDataObj -> itcastDataObj.getChargeStatus() == 2 || itcastDataObj.getChargeStatus() == 3);
        tripDriveDataStream.printToErr("驾驶行程数据>>>");

        //TODO 5：对驾驶行程数据应用水印（允许数据延迟30s）
        SingleOutputStreamOperator<ItcastDataObj> tripDriveWatermark = tripDriveDataStream.assignTimestampsAndWatermarks(
                new TripDriveWatermark());
        tripDriveWatermark.printToErr("添加完水印的数据>>>");

        //TODO 6：对加了水印的数据进行分组操作，应用窗口操作（session窗口）
        //TODO 6.1：车辆在驾驶行程中如果超过15m没有上报数据，可以认为是上一个行程的结束
        WindowedStream<ItcastDataObj, String, TimeWindow> tripDriveWindowStream = tripDriveWatermark.keyBy(ItcastDataObj::getVin)
                .window(EventTimeSessionWindows.withGap(Time.minutes(15)));

        //TODO 7：驾驶行程采样数据的业务开发
        //TODO 7.1：对数据应用自定义窗口的业务逻辑处理（划分四类数据：5s、10s、15s、20s）
        SingleOutputStreamOperator<String[]> driverSampleDataStream = tripDriveWindowStream.apply(new DriveSampleWindowFunction());
        driverSampleDataStream.printToErr("自定义窗口函数驾驶行程采样数据>>>");
        //TODO 7.2：将自定义驾驶行程采样业务处理后的数据写入到hbase中
        driverSampleDataStream.addSink(new TripSampleToHBaseSink("TRIPDB:trip_sample"));

        //TODO 8：驾驶行程数据的业务开发
        //TODO 8.1：驾驶行程数据应用自定义窗口的业务逻辑处理
        SingleOutputStreamOperator<TripModel> driverDataStream = tripDriveWindowStream.apply(new DriveTripWindowFunction());
        driverDataStream.printToErr("自定义窗口函数驾驶行程数据>>>");
        //TODO 8.2：将自定义驾驶行程业务处理后的数据写入到hbase中
        driverDataStream.addSink(new TripDriveToHBaseSink("TRIPDB:trip_division"));

        try {
            //TODO 9：递交作业
            env.execute();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
