package cn.itcast.streaming.function.window;

import cn.itcast.entity.CustomRuleAlarmResultModel;
import cn.itcast.entity.ItcastDataPartObj;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;
import java.util.ArrayList;

/**
 * 自定义告警规则窗口自定义函数开发
 */
public class CustomRuleAlarmWindowFunction implements WindowFunction<ItcastDataPartObj,
        ArrayList<ItcastDataPartObj>, String, TimeWindow> {

    /**
     * 自定义函数
     * @param s
     * @param timeWindow
     * @param iterable
     * @param collector
     * @throws Exception
     */
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<ItcastDataPartObj> iterable, Collector<ArrayList<ItcastDataPartObj>> collector) throws Exception {
        //将迭代器的数据转换成本地集合对象
        ArrayList<ItcastDataPartObj> resultList = Lists.newArrayList(iterable);
        resultList.sort((o1, o2)->{
            if(o1.getTerminalTimeStamp()> o2.getTerminalTimeStamp())
                return  1;
            else if (o1.getTerminalTimeStamp()< o2.getTerminalTimeStamp())
                return -1;
            else
                return 0;
        });
        collector.collect(resultList);
    }
}
