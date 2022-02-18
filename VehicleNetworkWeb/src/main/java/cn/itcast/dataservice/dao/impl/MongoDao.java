package cn.itcast.dataservice.dao.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:24
 * @Description TODO mongo使用mongoTemplate加载mongo服务访问数据
 */
@Repository
public class MongoDao {

    @Autowired
    private MongoTemplate mongoTemplate;

    public MongoTemplate getMongoTemplate() {
        return mongoTemplate;
    }

}