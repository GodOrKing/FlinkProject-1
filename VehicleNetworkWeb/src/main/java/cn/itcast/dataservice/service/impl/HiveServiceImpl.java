package cn.itcast.dataservice.service.impl;

import cn.itcast.dataservice.dao.impl.HiveJdbcDao;
import cn.itcast.dataservice.service.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:37
 * @Description TODO 编写hive测试接口，service实现类
 */
@Service
public class HiveServiceImpl implements HiveService {

    @Autowired
    private HiveJdbcDao hiveJdbcDao;

    @Override
    public Long totalNum() {
        Long total = hiveJdbcDao.getJdbcTemplate().queryForObject("select count(1) totalNum from itcast_src", Long.class);
        return total;
    }
}