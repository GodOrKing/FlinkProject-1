package cn.itcast.streaming.function.flatmap;

import cn.itcast.entity.ElectricFenceModel;
import cn.itcast.streaming.source.MysqlElectricFenceResultSource;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;

/**
 * 为电子围栏分析结果模型添加uuid和inMysql字段
 */
public class ElectricFenceModelFunction implements CoFlatMapFunction<ElectricFenceModel, HashMap<String, Long>, ElectricFenceModel> {
    private static Logger logger = LoggerFactory.getLogger(MysqlElectricFenceResultSource.class);

    HashMap<String, Long> electricFenceMapResult = new HashMap<>();

    @Override
    public void flatMap1(ElectricFenceModel electricFenceModel, Collector<ElectricFenceModel> collector) throws Exception {
        if(electricFenceMapResult.getOrDefault(electricFenceModel.getVin(), null) != null){
            //查找到了对应的进入电子围栏的数据
            Long electricFenceId = electricFenceMapResult.get(electricFenceModel.getVin());
            electricFenceModel.setUuid(electricFenceId);
            electricFenceModel.setInMysql(true);
        }else{
            //没有查找到电子围栏数据
            electricFenceModel.setUuid(Long.MAX_VALUE - new Date().getTime());
            electricFenceModel.setInMysql(false);
        }
        collector.collect(electricFenceModel);
    }

    @Override
    public void flatMap2(HashMap<String, Long> stringLongHashMap, Collector<ElectricFenceModel> collector) throws Exception {
        electricFenceMapResult = stringLongHashMap;
    }
}
