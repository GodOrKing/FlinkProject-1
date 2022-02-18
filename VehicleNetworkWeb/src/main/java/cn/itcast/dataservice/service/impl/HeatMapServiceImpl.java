package cn.itcast.dataservice.service.impl;

import cn.itcast.dataservice.bean.HeatMapPointBean;
import cn.itcast.dataservice.mapper.HeatMapMapper;
import cn.itcast.dataservice.service.HeatMapService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: laowei
 * @Date: 2020/9/16 1:52
 * @Description: TODO 热力图后台数据服务接口，service实现类
 */
@Service
public class HeatMapServiceImpl implements HeatMapService {

    @Autowired
    private HeatMapMapper heatMapMapper;

    @Override
    public List<List<Double>> queryPointsByCity(String city) {
        List<HeatMapPointBean> heatMapPointBeanList = heatMapMapper.queryPointsByCity(city);
        List<List<Double>> resultList = new ArrayList<>();
        heatMapPointBeanList.forEach(heatMapPointBean -> {
            List<Double> points = new ArrayList<>(2);
            points.add(heatMapPointBean.getLongitude());
            points.add(heatMapPointBean.getLatitude());
            resultList.add(points);

        });
        return resultList;
    }

    @Override
    public List<List<Double>> queryAllPoints() {
        List<HeatMapPointBean> heatMapPointBeanList = heatMapMapper.queryAllPoints();
        List<List<Double>> resultList = new ArrayList<>();
        heatMapPointBeanList.forEach(heatMapPointBean -> {
            List<Double> points = new ArrayList<>(2);
            points.add(heatMapPointBean.getLongitude());
            points.add(heatMapPointBean.getLatitude());
            resultList.add(points);

        });
        return resultList;
    }
}