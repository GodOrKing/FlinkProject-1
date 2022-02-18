package cn.itcast.dataservice.service;

import cn.itcast.dataservice.bean.ItcastDataRateBean;

import java.util.List;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 2:01
 * @Description TODO
 */
public interface ItcastDataRateService {
    List<ItcastDataRateBean> queryAll(int pageNo, int pageSize);
    Long totalNum();
}
