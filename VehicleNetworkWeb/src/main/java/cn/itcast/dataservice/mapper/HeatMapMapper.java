package cn.itcast.dataservice.mapper;

import cn.itcast.dataservice.bean.HeatMapPointBean;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:51
 * @Description TODO 热力图后台数据服务接口Mapper接口，对应xml文件名称
 */
@Repository
public interface HeatMapMapper {
    List<HeatMapPointBean> queryPointsByCity(String city);
    List<HeatMapPointBean> queryAllPoints();
}