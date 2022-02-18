package cn.itcast.streaming.watermark;

import cn.itcast.entity.CustomRuleAlarmResultModel;
import cn.itcast.entity.ItcastDataPartObj;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 定义自定义告警规则的水印操作
 */
public class CustomRuleAlarmWatermark implements AssignerWithPeriodicWatermarks<ItcastDataPartObj> {
    //当前窗口最大的时间戳
    Long currentMaxTimestamp = 0L;
    //定义允许最大乱序的事件：30s
    Long maxOutOfOrderness = 1000*30L;

    /**
     * 返回最新的水印
     * @return
     */

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp- maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(ItcastDataPartObj itcastDataPartObj, long l) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, itcastDataPartObj.getTerminalTimeStamp());
        return itcastDataPartObj.getTerminalTimeStamp();
    }
}
