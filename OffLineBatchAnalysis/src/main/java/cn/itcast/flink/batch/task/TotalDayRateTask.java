package cn.itcast.flink.batch.task;

import cn.itcast.flink.batch.AnalysisDataRate;
import cn.itcast.flink.batch.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 按天统计数据的准确率
 */
public class TotalDayRateTask {
    private static Logger logger = LoggerFactory.getLogger(TotalDayRateTask.class);

    public static void main(String[] args) {
        String day = DateUtil.getTodayDate();
        AnalysisDataRate dataRate = new AnalysisDataRate(day);
        logger.info("运行按照天统计数据的准确率："+day);
        long time = System.currentTimeMillis();
        dataRate.executeTask();
        logger.info("TotalDayRateTask end, executing time:{}", System.currentTimeMillis() - time);
    }

}
