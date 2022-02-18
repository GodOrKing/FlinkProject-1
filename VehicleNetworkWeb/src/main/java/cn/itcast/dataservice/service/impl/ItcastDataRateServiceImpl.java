package cn.itcast.dataservice.service.impl;

import cn.itcast.dataservice.bean.ItcastDataRateBean;
import cn.itcast.dataservice.mapper.ItcastDataRateMapper;
import cn.itcast.dataservice.service.ItcastDataRateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 2:01
 * @Description TODO 数据正确率和错误率后台数据服务接口，service实现类
 */
@Service
public class ItcastDataRateServiceImpl implements ItcastDataRateService {
    @Autowired
    private ItcastDataRateMapper itcastDataRateMapper;

    @Override
    public List<ItcastDataRateBean> queryAll(int pageNo, int pageSize) {
        if (pageNo <= 1) {
            pageNo = 0;
        } else {
            pageNo -= 1;
        }
        return itcastDataRateMapper.queryAll(pageNo * pageSize, pageSize);
    }

    @Override
    public Long totalNum() {
        return itcastDataRateMapper.totalNum();
    }
}