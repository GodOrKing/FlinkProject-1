package cn.itcast.dataservice.service;

import cn.itcast.dataservice.bean.ElectronicFenceVinsBean;

import java.util.List;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:33
 * @Description TODO 编写Mysql测试接口service层
 */
public interface MySqlService {
    // 分页查询电子围栏信息
    List<ElectronicFenceVinsBean> queryAll(int pageNo, int pageSize);
    // 查询电子围栏总数
    Long totalNum();
}