package cn.itcast.streaming.task;

import cn.itcast.entity.CustomRuleAlarmResultModel;
import cn.itcast.entity.ItcastDataPartObj;
import cn.itcast.entity.RuleInfoModel;
import cn.itcast.entity.VehicleInfoModel;
import cn.itcast.streaming.function.asyncio.AsyncHttpQueryFunction;
import cn.itcast.streaming.function.flatmap.CustomRuleAlarmExpressionFunction;
import cn.itcast.streaming.function.flatmap.CustomRuleAlarmVehicleInfoFunction;
import cn.itcast.streaming.function.map.LocationInfoRedisFunction;
import cn.itcast.streaming.function.window.CustomRuleAlarmWindowFunction;
import cn.itcast.streaming.sink.CustomRuleAlarmMongoSink;
import cn.itcast.streaming.source.MysqlRuleInfoSource;
import cn.itcast.streaming.source.MysqlVehicleInfoFunction;
import cn.itcast.streaming.watermark.CustomRuleAlarmWatermark;
import cn.itcast.utils.JsonParsePartUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * 自定义规则告警实时业务开发
 * 实时读取kafka数据，将数据读取出来以后应用自定义告警规则后，将结果数据写入到mongodb数据库中
 */
public class CustomRuleAlarmTask  extends BaseTask{
    /**
     * 入口方法
     * @param args
     */
    public static void main(String[] args) {
        /**
         * 实现步骤：
         * 1）创建flink流式运行环境（hadoophome_name、事件时间处理数据、checkpoint）
         * 2）接入kafka数据源消费数据
         * 3）将消费到的json字符串转换成javaBean对象
         * 4）过滤掉异常数据，保留正常数据
         * 5）与redis维度表进行关联拉宽地理位置信息，对拉宽后的流数据关联redis，根据geohash找到地理位置信息，进行拉宽操作
         * 6）过滤出来redis拉宽成功的地理位置数据
         * 7）过滤出来redis拉宽失败的地理位置数据
         * 8）对redis拉宽失败的地理位置数据使用异步io访问高德地图逆地理位置查询地理位置信息，并将返回结果写入到redis中
         * 9）将reids拉宽的地理位置数据与高德api拉宽的地理位置数据进行合并
         * 10）创建vin的分组操作并进行90s的基于事件时间的滚动窗口
         * 11）查询并加载车辆明细表数据，加载后进行广播操作
         * 12）将第10步的窗口流数据与第11步的广播流数据进行关联，根据车辆基础表数据拉宽窗口流数据
         * 13）加载自定义告警规则数据并进行广播
         * 14）将拉宽后的数据与自定义告警规则广播流数据进行逻辑处理，找出来异常数据（告警数据）
         * 15）将最终计算结果写入到mongodb数据库中
         * 16）运行任务，递交作业
         */

        //TODO 1）创建flink流式运行环境（hadoophome_name、事件时间处理数据、checkpoint）
        StreamExecutionEnvironment env = getEnv(CustomRuleAlarmTask.class.getSimpleName());

        //TODO 2）接入kafka数据源消费数据
        DataStream<String> kafkaStream = createKafkaStream(SimpleStringSchema.class);
        kafkaStream.print("原始数据>>>");

        //TODO 3）将消费到的json字符串转换成javaBean对象
        SingleOutputStreamOperator<ItcastDataPartObj> itcastDataPartObjDataStream = kafkaStream
                .filter(jsonObj-> StringUtils.isNotEmpty(jsonObj)).map(JsonParsePartUtil::parseJsonToObject);

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

        //TODO 10）创建vin的分组操作并进行90s的基于事件时间的滚动窗口
        SingleOutputStreamOperator<ArrayList<ItcastDataPartObj>> srcDataPartWindowStream = itcastDataObjLocationStream
                .assignTimestampsAndWatermarks(new CustomRuleAlarmWatermark())
                .keyBy(itcastDataPartObj -> itcastDataPartObj.getVin())
                .window(TumblingEventTimeWindows.of(Time.seconds(90)))
                .apply(new CustomRuleAlarmWindowFunction());

        //TODO 11）查询并加载车辆明细表数据，加载后进行广播操作
        DataStream<HashMap<String, VehicleInfoModel>> vehicleInfoDataStream = env.addSource(new MysqlVehicleInfoFunction()).broadcast();

        //TODO 12）将第10步的窗口流数据与第11步的广播流数据进行关联，根据车辆基础表数据拉宽窗口流数据
        SingleOutputStreamOperator<ArrayList<Tuple2<ItcastDataPartObj, VehicleInfoModel>>> vehicleConnectDataStream = srcDataPartWindowStream
                .connect(vehicleInfoDataStream)
                .flatMap(new CustomRuleAlarmVehicleInfoFunction());
        vehicleConnectDataStream.print("拉宽车辆基础信息表后的数据>>>");

        //TODO 13）加载自定义告警规则数据并进行广播
        DataStream<ArrayList<RuleInfoModel>> ruleInfoDataStream = env.addSource(new MysqlRuleInfoSource()).broadcast();
        ruleInfoDataStream.printToErr("自定义告警规则流数据>>>");

        //TODO 14）将拉宽后的数据与自定义告警规则广播流数据进行逻辑处理，找出来异常数据
        SingleOutputStreamOperator<ArrayList<CustomRuleAlarmResultModel>> resultDataStream = vehicleConnectDataStream
                .connect(ruleInfoDataStream).flatMap(new CustomRuleAlarmExpressionFunction());

        //TODO 15）将最终计算结果写入到mongodb数据库中
        resultDataStream.print("分析后的结果数据>>>");
        resultDataStream.addSink(new CustomRuleAlarmMongoSink());

        try {
            //TODO 16）运行任务，递交作业
            env.execute();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
