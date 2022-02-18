package cn.itcast.streaming.task;

import cn.itcast.entity.ItcastDataPartObj;
import cn.itcast.entity.OnlineDataObj;
import cn.itcast.entity.VehicleInfoModel;
import cn.itcast.streaming.function.asyncio.AsyncHttpQueryFunction;
import cn.itcast.streaming.function.flatmap.VehicleInfoMapMysqlFunction;
import cn.itcast.streaming.function.map.LocationInfoRedisFunction;
import cn.itcast.streaming.function.window.OnlineStatisticsWindowFunction;
import cn.itcast.streaming.sink.OnlineStatisticsToMysqlSink;
import cn.itcast.streaming.source.MysqlVehicleInfoFunction;
import cn.itcast.streaming.watermark.OnlineStatisticsWatermark;
import cn.itcast.utils.JsonParsePartUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * 在线故障实时分析业务开发
 * 消费kafka的数据进行在线故障实时分析，分析结果写入到mysql数据库中
 */
public class OnlineStatisticsTask extends BaseTask {
    private static Logger logger = LoggerFactory.getLogger(OnlineStatisticsTask.class);
    /**
     * 入口方法
     * @param args
     */
    public static void main(String[] args) {
        /**
         * 实现步骤：
         * 1）初始化flink流处理的运行环境（事件时间、checkpoint、hadoop name）
         * 2）接入kafka数据源，消费kafka数据
         * 3）将消费到的json字符串转换成javaBean对象
         * 4）过滤掉异常数据，保留正常数据
         * 5）与redis维度表进行关联拉宽地理位置信息，对拉宽后的流数据关联redis，根据geohash找到地理位置信息，进行拉宽操作
         * 6）过滤出来redis拉宽成功的地理位置数据
         * 7）过滤出来redis拉宽失败的地理位置数据
         * 8）对redis拉宽失败的地理位置数据使用异步io访问高德地图逆地理位置查询地理位置信息，并将返回结果写入到redis中
         * 9）将reids拉宽的地理位置数据与高德api拉宽的地理位置数据进行合并
         * 10）创建原始数据的30s的滚动窗口，根据vin进行分流操作
         * 11）对原始数据的窗口流数据进行实时故障分析（区分出来告警数据和非告警数据19个告警字段）
         * 12）加载业务中间表（7张表：车辆表、车辆类型表、车辆销售记录表，车俩用途表4张），并进行广播
         * 13）将第11步和第12步的广播流结果进行关联，并应用拉宽操作
         * 14）将拉宽后的结果数据写入到mysql数据库中
         * 15）启动作业
         */

        //TODO 1）初始化flink流处理的运行环境（事件时间、checkpoint、hadoop name）
        StreamExecutionEnvironment env = getEnv(OnlineStatisticsTask.class.getSimpleName());

        //TODO 2）接入kafka数据源，消费kafka数据
        DataStream<String> kafkaStream = createKafkaStream(SimpleStringSchema.class);
        kafkaStream.print("原始数据>>>");

        //TODO 3）将消费到的json字符串转换成javaBean对象
        SingleOutputStreamOperator<ItcastDataPartObj> itcastDataPartObjDataStream = kafkaStream.filter(jsonObj-> StringUtils.isNotEmpty(jsonObj)).map(JsonParsePartUtil::parseJsonToObject);

        //TODO 4）过滤掉异常数据，保留正常数据
        SingleOutputStreamOperator<ItcastDataPartObj> itcastDataObjStream = itcastDataPartObjDataStream
                .filter(itcastDataPartObj -> itcastDataPartObj.getErrorData().isEmpty());
        itcastDataObjStream.print("正确数据>>>");

        //TODO 5）与redis维度表进行关联拉宽地理位置信息，对拉宽后的流数据关联redis，根据geohash找到地理位置信息，进行拉宽操作
        SingleOutputStreamOperator<ItcastDataPartObj> itcastLocationDataObjStream = itcastDataObjStream.map(
                new LocationInfoRedisFunction());

        //TODO 6）过滤出来redis拉宽成功的地理位置数据
        SingleOutputStreamOperator<ItcastDataPartObj> itcastDataObjWithLocationDataStream = itcastLocationDataObjStream.filter(
                itcastDataPartObj -> StringUtils.isNotEmpty(itcastDataPartObj.getProvince()));
        itcastDataObjWithLocationDataStream.print("与redis存储的地理位置数据拉宽后的结果>>>");

        //TODO 7）过滤出来redis拉宽失败的地理位置数据
        SingleOutputStreamOperator<ItcastDataPartObj> itcastDataObjNoWithLocationDataStream = itcastLocationDataObjStream.filter(
                itcastDataPartObj -> StringUtils.isEmpty(itcastDataPartObj.getProvince()));

        //TODO 8）对redis拉宽失败的地理位置数据使用异步io访问高德地图逆地理位置查询地理位置信息，并将返回结果写入到redis中
        SingleOutputStreamOperator<ItcastDataPartObj> asyncItcastDataObjWithLocationDataStream = AsyncDataStream.unorderedWait(
                itcastDataObjNoWithLocationDataStream,    //输入的数据流
                new AsyncHttpQueryFunction(),       //异步查询的function对象
                2000,                       //超时时间
                TimeUnit.MILLISECONDS,              //超时的时间单位
                10                          //最大的异步并发请求数量（异步的并发线程队列数）
        );
        asyncItcastDataObjWithLocationDataStream.print("与高德api的地理位置数据拉宽后的结果>>>");

        //TODO 9）将reids拉宽的地理位置数据与高德api拉宽的地理位置数据进行合并
        DataStream<ItcastDataPartObj> itcastDataObjLocationStream = itcastDataObjWithLocationDataStream.union(asyncItcastDataObjWithLocationDataStream);
        itcastDataObjLocationStream.printToErr("拉宽后的带有地理位置信息的数据>>>");

        //TODO 10）创建原始数据的30s的滚动窗口，根据vin进行分流操作
        WindowedStream<ItcastDataPartObj, String, TimeWindow> itcastWindowDataStream = itcastDataObjLocationStream
                .assignTimestampsAndWatermarks(new OnlineStatisticsWatermark())
                .keyBy(itcastDataPartObj -> itcastDataPartObj.getVin())
                .window(TumblingEventTimeWindows.of(Time.seconds(30)));

        //TODO 11）对原始数据的窗口流数据进行实时故障分析（区分出来告警数据和非告警数据19个告警字段）
        SingleOutputStreamOperator<OnlineDataObj> onlineDataObjDataStrem = itcastWindowDataStream.apply(new OnlineStatisticsWindowFunction());
        onlineDataObjDataStrem.printToErr("拉宽后的在线故障实时分析数据>>>");

        //TODO 12）加载业务中间表（7张表：车辆表、车辆类型表、车辆销售记录表，车俩用途表4张），并进行广播
        DataStream<HashMap<String, VehicleInfoModel>> vehicleInfoDataStream = env.addSource(new MysqlVehicleInfoFunction()).broadcast();

        //TODO 13）将第11步和第12步的广播流结果进行关联，并应用拉宽操作
        SingleOutputStreamOperator<OnlineDataObj> connectVehicleDataStream = onlineDataObjDataStrem.connect(vehicleInfoDataStream)
                .flatMap(new VehicleInfoMapMysqlFunction());

        //TODO 14）将拉宽后的结果数据写入到mysql数据库中
        connectVehicleDataStream.addSink(new OnlineStatisticsToMysqlSink());

        try {
            //TODO 15）启动作业
            env.execute();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
