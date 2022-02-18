package cn.itcast.dataservice.service;

import java.util.List;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:53
 * @Description TODO 热力图后台数据服务接口,服务接口类
 */
public interface HeatMapService {
    List<List<Double>> queryPointsByCity(String city);
    List<List<Double>> queryAllPoints();
}