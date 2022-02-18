package cn.itcast.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import java.util.LinkedList;
import java.util.List;

/**
 * @Description:副本集客户端访问测试
 */
public class MongoClusterClientTest {
    private static MongoClient mongoClient =null;

    public static void main(String[] args) {
        MongoClusterClientTest clusterClientTest = new MongoClusterClientTest();
        MongoClient mongoClient = clusterClientTest.getMongoClient();
        System.out.println(mongoClient.getReplicaSetStatus());
    }

    /**
     * @desc 获得mongo副本集集群服务器客户端连接
     * @return MongoClient
     */
    private MongoClient getMongoClient() {
        List hosts = new LinkedList<ServerAddress>();
        hosts.add(new ServerAddress("node01",27017));
        hosts.add(new ServerAddress("node02",27017));
        hosts.add(new ServerAddress("node03",27017));
        if (mongoClient==null) {
            mongoClient = new MongoClient(hosts);
        }
        return mongoClient;
    }

    /**
     * @desc 关闭客户端连接
     */
    private void close() {
        mongoClient.close();
    }
}
