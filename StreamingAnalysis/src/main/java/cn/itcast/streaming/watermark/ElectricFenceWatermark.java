package cn.itcast.streaming.watermark;

import cn.itcast.entity.ElectricFenceModel;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 电子围栏水印自定义操作
 */
public class ElectricFenceWatermark implements AssignerWithPeriodicWatermarks<ElectricFenceModel> {
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
    public long extractTimestamp(ElectricFenceModel electricFenceModel, long l) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, electricFenceModel.getTerminalTimestamp());
        return electricFenceModel.getTerminalTimestamp();
    }
}
