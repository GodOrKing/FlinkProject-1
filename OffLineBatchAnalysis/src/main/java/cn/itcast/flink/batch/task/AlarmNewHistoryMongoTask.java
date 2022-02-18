package cn.itcast.flink.batch.task;

import cn.itcast.flink.batch.utils.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.LinkedList;
import java.util.List;

/**
 * @Description:自定义告警规则新增告警与历史告警统计
 */
public class AlarmNewHistoryMongoTask {

    public static void main(String[] args) {
        // 1.获得mongo客户端连接
        MongoClient mongoClient = MongoUtil.getConnect();
        // 2.获得被操作的数据库：itcast
        MongoDatabase database = mongoClient.getDatabase("itcast");
        // 3.获得被操作的集合：custom_rule_alarm
        MongoCollection<Document> customRuleAlarm = database.getCollection("custom_rule_alarm");
        // 4.sql实现
        List aggregateList = new LinkedList();
        Document doc = new Document();
        doc.append("$project", BasicDBObject.parse("{\"countNum\":1,\"dataLabel\":{ $cond: { if: { $eq: [ \"$state\", 1 ] }, then:\"新增告警数据\", else:\"历史告警数据\"}}}"));
        aggregateList.add(doc);
        doc = new Document();
        doc.append("$group", BasicDBObject.parse("{\"_id\":{\"dataLabel\":\"$dataLabel\"},\"countNum\":{$sum:1}}"));
        aggregateList.add(doc);
        doc = new Document();
        doc.append("$sort", BasicDBObject.parse("{\"countNum\" : -1}"));
        aggregateList.add(doc);
        System.out.println(aggregateList.toString());
        MongoCursor mongoCursor = customRuleAlarm.aggregate(aggregateList).iterator();
        while (mongoCursor.hasNext()) {
            System.out.println(mongoCursor.next());
        }
    }
}