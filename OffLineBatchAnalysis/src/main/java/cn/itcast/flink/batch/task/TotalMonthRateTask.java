package cn.itcast.flink.batch.task;

import cn.itcast.flink.batch.AnalysisDataRate;
import cn.itcast.flink.batch.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按照月计算数据的准确率
 */
public class TotalMonthRateTask {
    private static Logger logger = LoggerFactory.getLogger(TotalMonthRateTask.class);

    public static void main(String[] args) {
        String month = DateUtil.getYearMonthDate() + "00";
        AnalysisDataRate dataRate = new AnalysisDataRate(month);
        logger.info("运行按照月统计数据的准确率："+month);
        long time = System.currentTimeMillis();
        dataRate.executeTask();
        logger.info("TotalMonthRateTask end, executing time:{}", System.currentTimeMillis() - time);
    }
}
