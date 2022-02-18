package cn.itcast.dataservice.controller;

import cn.itcast.dataservice.annotation.AutoResponse;
import cn.itcast.dataservice.annotation.LogAudit;
import cn.itcast.dataservice.service.HiveService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:37
 * @Description TODO Hive数据源测试类，查询车辆信息数据后台数据服务接口
 */
@Slf4j
@RestController
@Api(value = "Hive数据源测试")
public class HiveController {

    @Autowired
    private HiveService hiveService;

    @LogAudit
    @AutoResponse
    @ApiOperation(value = "查询原始正确数据的总数", response = Long.class, responseContainer = "Long")
    @RequestMapping(value = "hive/totalNum", produces = { "application/json" }, method = RequestMethod.GET)
    public Object totalNum() {
        try {
            Long totalNum = hiveService.totalNum();
            return totalNum;
        } catch (Throwable t) {
            log.error("failed to sql databases.\n", t);
            return null;
        }
    }
}
