package cn.itcast.dataservice.mapper;

import cn.itcast.dataservice.bean.ItcastDataRateBean;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 2:00
 * @Description TODO 数据正确率和错误率后台数据服务接口Mapper接口，对应xml文件名称
 */
@Repository
public interface ItcastDataRateMapper {
    // TODO @Param注解 :明确标识param中的值为某一个参数名称:pageNo
    List<ItcastDataRateBean> queryAll(@Param("pageNo") int pageNo, @Param("pageSize") int pageSize);
    Long totalNum();
}