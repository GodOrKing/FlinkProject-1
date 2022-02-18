package cn.itcast.dataservice.service;

import cn.itcast.dataservice.bean.ElectricFenceBean;

import java.util.List;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 2:04
 * @Description TODO 电子围栏后台数据服务接口,服务接口类
 */
public interface ElectricFenceService {
    List<ElectricFenceBean> queryAll(Integer pageNo, Integer pageSize);
    Long totalNum();
}