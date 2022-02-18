package cn.itcast.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.List;

/**
 * mongodb操作的工具类
 */
public class MongoUtil {
    /**
     * 返回mongodb的客户端对象
     * @return
     */
    public static MongoClient getConnect() {
        //获取mongodb的hostname
        String host = ConfigLoader.getProperty("mongo.host");
        //mongodb的端口号
        int port = Integer.parseInt(ConfigLoader.getProperty("mongo.port"));
        //用户名
        String userName = ConfigLoader.getProperty("mongo.userName");
        //密码
        String password = ConfigLoader.getProperty("mongo.password");
        //数据源
        String source = ConfigLoader.getProperty("mongo.source");
        //指定地址和端口号获取服务器地址对象
        ServerAddress serverAddress = new ServerAddress(host, port);

        List<MongoCredential> credential = new ArrayList<MongoCredential>();
        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential mongoCredential1 = MongoCredential.createScramSha1Credential(userName, source, password.toCharArray());
        credential.add(mongoCredential1);

        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(serverAddress, credential);
        return mongoClient;
    }
}
