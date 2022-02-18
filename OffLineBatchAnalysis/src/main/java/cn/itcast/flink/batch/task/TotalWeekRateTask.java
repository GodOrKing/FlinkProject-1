package cn.itcast.flink.batch.task;

import cn.itcast.flink.batch.AnalysisDataRate;
import cn.itcast.flink.batch.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按照周计算数据准确率
 */
public class TotalWeekRateTask {
    private static Logger logger = LoggerFactory.getLogger(TotalWeekRateTask.class);

    public static void main(String[] args) {
        String week = DateUtil.getNowWeekStart();
        AnalysisDataRate dataRate = new AnalysisDataRate(week);
        logger.info("运行按照周统计数据的准确率："+week);
        long time = System.currentTimeMillis();
        dataRate.executeTask();
        logger.info("TotalWeekRateTask end, executing time:{}", System.currentTimeMillis() - time);
    }
}
