package cn.itcast.streaming.watermark;

import cn.itcast.entity.ItcastDataObj;
import org.apache.flink.streaming.api.functions.*;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 驾驶行程自定义水印对象，解决数据迟到30s的问题
 */
public class TripDriveWatermark implements AssignerWithPeriodicWatermarks<ItcastDataObj> {
    //定义当前窗口最大的水印时间戳
    Long currentMaxTimestamp = 0L;
    //定义允许最大乱序的事件：30s
    Long maxOutOfOrderness = 1000*30L;

    /**
     * 生成当前水印
     * @return
     */
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    /**
     * 抽取事件事件
     * @param itcastDataObj
     * @param l
     * @return
     */
    @Override
    public long extractTimestamp(ItcastDataObj itcastDataObj, long l) {
        //获取到当前窗口最大的事件时间
        currentMaxTimestamp = Math.max(currentMaxTimestamp, itcastDataObj.getTerminalTimeStamp());
        System.out.println("当前数据时间："+itcastDataObj.getTerminalTimeStamp());
        return currentMaxTimestamp;
    }
}
