package cn.itcast.streaming.sink;

import cn.itcast.entity.CustomRuleAlarmResultModel;
import cn.itcast.utils.DateUtil;
import cn.itcast.utils.MongoUtil;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

/**
 * TODO 自定义告警规则mongo数据
 */
public class CustomRuleAlarmMongoSink extends RichSinkFunction<ArrayList<CustomRuleAlarmResultModel>> {

    //初始化mongoclient客户端对象
    private MongoClient mongoClient = null;

    /**
     * 初始化资源
     * @param parameters
     */
    @Override
    public void open(Configuration parameters) {
        mongoClient = MongoUtil.getConnect();
    }

    /**
     * 将数据一条条的写入到mongo数据库中
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(ArrayList<CustomRuleAlarmResultModel> value, Context context) throws Exception {
        try {
            // todo mongo数据库名称和分析结果集合的名称，是提前定义的
            //获取当前操作的数据库
            MongoDatabase db = mongoClient.getDatabase("itcast");
            //获取到当前操作的表
            MongoCollection<Document> collection = db.getCollection("custom_rule_alarm");
            // todo 使用java8 stream groupingBy方式处理分组
                    // todo Collectors.groupingBy(tuple14 -> tuple14.f11) 根据Tuple14中的monitorId进行分组，得到Collector对象,
                    //      然后通过要被排序的集合stream方法，成为Stream对象，调用collect方法(参数：Collector对象)，返回值为Map<T,V>
                    // todo 按照monitor_id分组,在tuple14中的第十二个元素 monitorId
            //在某些情况下，某个车架号的某个窗口的数据可能同时满足了多个监控类型，因此需要根据监控类型进行分组，同一个监控类型的数据放到一个组中
            Map<Integer, List<CustomRuleAlarmResultModel>> ruleAlarmResultModelListMap = value.stream().collect(
                    Collectors.groupingBy(ruleAlarmResultModel -> ruleAlarmResultModel.getMonitorId()));

            // todo 得到的是map中的所有key
            Set<Integer> keySet = ruleAlarmResultModelListMap.keySet();
            Iterator<Integer> keyIterator = keySet.iterator();
            //循环遍历每一个监控类型id（满足了几个监控类型id就循环几次）
            while (keyIterator.hasNext()) {
                // todo 根据迭代器中的key，拿到map的value值(List<Tuple14>)
                LinkedList<CustomRuleAlarmResultModel> ruleAlarmResultModelList = Lists.newLinkedList(
                        ruleAlarmResultModelListMap.get(keyIterator.next()));

                // todo 同一个key下的list（key为monitorId，list中是同一个监控类型的多条车辆数据）进行排序
                ruleAlarmResultModelList.sort((o1, o2) -> {
                    // todo 根据tuple14中的第二个元素terminalTime进行排序
                    long terminalTime1 = DateUtil.convertStringToDate(o1.getTerminalTime()).getTime();
                    long terminalTime2 = DateUtil.convertStringToDate(o2.getTerminalTime()).getTime();
                    if (terminalTime1 > terminalTime2) {
                        return 1;
                    } else if (terminalTime1 < terminalTime2) {
                        return -1;
                    } else {
                        return 0;
                    }
                });

                // todo 计数(触发报警规则数据条数计数器)
                int count = 0;
                for (int i = 0; i < ruleAlarmResultModelList.size(); i++) {
                    // todo 拿到同一个类型下具体的车辆数据
                    CustomRuleAlarmResultModel ruleAlarmResultModel = ruleAlarmResultModelList.get(i);
                    Document doc = new Document();
                    // todo 插入状态标识为1
                    doc.append("vin", ruleAlarmResultModel.getVin());   //告警的车辆
                    doc.append("start_time", ruleAlarmResultModel.getTerminalTime()); //告警开始时间
                    doc.append("end_time", null);                                     //告警结束时间
                    doc.append("rule_name", ruleAlarmResultModel.getRuleName());      //告警对应的规则
                    doc.append("series_name", ruleAlarmResultModel.getSeriesNameValue()); //告警车辆所属的车系名称
                    doc.append("series_code", ruleAlarmResultModel.getSeriesCodeValue()); //告警车辆所属的车系编码
                    doc.append("model_name", ruleAlarmResultModel.getModelNameValue());   //告警车辆所属的车型名称
                    doc.append("model_code", ruleAlarmResultModel.getModelCodeValue());   //告警车辆所属的车型编码
                    doc.append("province", ruleAlarmResultModel.getProvince());           //告警车辆所在的省份
                    doc.append("city", ruleAlarmResultModel.getCity());                   //告警车辆所在的城市
                    doc.append("monitor_id", ruleAlarmResultModel.getMonitorId());        //监控id
                    doc.append("lat", ruleAlarmResultModel.getLat());                     //告警车辆所在的经度
                    doc.append("lng", ruleAlarmResultModel.getLng());                     //告警车辆所在的纬度
                    doc.append("state", 1);                                               //告警的状态 1：告警中  0：告警结束，end_time更新了值
                    doc.append("process_time", DateUtil.getCurrentDate());
                    // todo 更新状态标识为0
                    Document docUpdate = new Document();
                    if(ruleAlarmResultModelList.getLast() == ruleAlarmResultModel) {
                        docUpdate.append("end_time", ruleAlarmResultModel.getTerminalTime());
                    }else{
                        docUpdate.append("end_time", ruleAlarmResultModelList.get(i + 1).getTerminalTime());
                    }
                    docUpdate.append("state", 0);
                    docUpdate.append("process_time", DateUtil.getCurrentDate());
                    // todo 根据vin monitor_id 查询 find(param) param: {vin:"tuple14.f0的值","monitor_id:tuple14.f11"}
                    BasicDBObject temp = new BasicDBObject("vin", ruleAlarmResultModel.getVin()).append("monitor_id",
                            ruleAlarmResultModel.getMonitorId());

                    /*<-***********************************************如果状态标识为1则表示触发报警规则*********************************************** ->*/
                    if (1 == ruleAlarmResultModel.getAlarmFlag()) {
                        count = count + 1;
                        // todo 1 如果计数器值等于帧数值
                        if (count == ruleAlarmResultModel.getAlarmFrame()) {
                            // todo 1.1 mongo存在数据
                            MongoCursor<Document> documentMongoCursor = collection.find(temp).iterator();
                            if (documentMongoCursor.hasNext()) {
                                //获取到mongodb数据库中的最后一条数据（某个vin以及某个监控id的最后以后）
                                Document document1 = collection.find(temp).sort(new BasicDBObject("start_time", -1)).first();
                                //如果最后一条数据的状态时（0：告警结束，end_time更新了值），意味着上一个告警周期的结束
                                if (document1.getInteger("state") == 0) {
                                    //追加一条新的数据表示下一个告警周期的开始
                                    collection.insertOne(doc);
                                    if((ruleAlarmResultModelList.getLast() != ruleAlarmResultModel)
                                            && ruleAlarmResultModelList.get(i + 1).getAlarmFlag() == 0) {
                                        //重置告警次数计数器为0
                                        count = 0;
                                        //将当前告警周期结束的时间更新掉
                                        Document document2 = collection.find(temp).sort(new BasicDBObject("start_time", -1)).first();
                                        collection.updateOne(document2, new Document("$set", docUpdate));
                                    }
                                }
                                // todo status==1表示上个窗口最新一条数据是开始时间 则需要判断下一个窗口数据是否连续
                                else if (document1.getInteger("state") == 1) {
                                    // todo i - ruleAlarmResultModel.getAlarmFrame() == -1表示连续不做处理
                                    //  >-1表示不连续存在结束时间 需要设置结束时间并重置开始时间
                                    if (i - ruleAlarmResultModel.getAlarmFrame() > -1) {
                                        boolean flag = true;
                                        for (int j = 0; j <= (i - ruleAlarmResultModel.getAlarmFrame()); j++) {
                                            if (flag) {
                                                if (ruleAlarmResultModelList.get(j).getAlarmFlag() == 0) {
                                                    // todo 1.1.1更新
                                                    flag = false;
                                                    Document updateDoc = new Document();
                                                    updateDoc.append("end_time", ruleAlarmResultModelList.get(j).getTerminalTime());
                                                    updateDoc.append("state", 0);
                                                    updateDoc.append("process_time", DateUtil.getCurrentDate());
                                                    collection.updateOne(document1, new Document("$set", updateDoc));
                                                    // todo 1.1.2 重置开始时间
                                                    collection.insertOne(doc);
                                                    if((ruleAlarmResultModelList.getLast() != ruleAlarmResultModel)
                                                            && ruleAlarmResultModelList.get(i + 1).getAlarmFlag() == 0) {
                                                        count = 0;
                                                        // todo 更新状态标识为0 根据vin和monitorId查询结果，且根据startTime进行倒序排序，获得查询结果的第一条数据
                                                        Document document3 = collection.find(temp).sort(new BasicDBObject("start_time", -1)).first();
                                                        collection.updateOne(document3, new Document("$set", docUpdate));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                //在mongod中没有查询到数据
                            } else {
                                // todo 1.2 mongo没有数据表示第一次插入
                                collection.insertOne(doc);
                                if((ruleAlarmResultModelList.getLast() != ruleAlarmResultModel) && ruleAlarmResultModelList.get(i + 1).getAlarmFlag() == 0) {
                                    count = 0;
                                    // todo 更新状态标识为0 根据vin和monitorId查询结果，且根据startTime进行倒序排序，获得查询结果的第一条数据
                                    Document documet4 = collection.find(temp).sort(new BasicDBObject("start_time", -1)).first();
                                    collection.updateOne(documet4, new Document("$set", docUpdate));
                                }
                            }
                        }
                        // todo 2，如果计数器值小于帧数值 判断告警车辆的数据此时 < 页面设置告警信息的次数
                        else if (count < ruleAlarmResultModel.getAlarmFrame()) {
                            //如果下一条数据是非告警状态则进行重新计数
                            //如果当前这条数据不是最后一条，且下一条数据是非告警数据
                            if((ruleAlarmResultModelList.getLast() != ruleAlarmResultModel) && ruleAlarmResultModelList.get(i + 1).getAlarmFlag() == 0) {
                                count = 0;
                            }
                        }
                        // todo 3,如果计数器值大于帧数值
                        else if (count > ruleAlarmResultModel.getAlarmFrame()) {
                            if((ruleAlarmResultModelList.getLast() != ruleAlarmResultModel) && ruleAlarmResultModelList.get(i + 1).getAlarmFlag() == 0) {
                                count = 0;
                                // todo 根据vin和monitorId查询结果，且根据startTime进行倒序排序，获得查询结果的第一条数据 查询非告警信息的第一条数据
                                Document document5 = collection.find(temp).sort(new BasicDBObject("start_time", -1)).first();
                                collection.updateOne(document5, new Document("$set", docUpdate));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
