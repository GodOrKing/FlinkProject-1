package cn.itcast.dataservice.controller;

import cn.itcast.dataservice.annotation.AutoResponse;
import cn.itcast.dataservice.annotation.LogAudit;
import cn.itcast.dataservice.service.VehicleViewService;
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
 * @Date 2020/9/17 23:13
 * @Description TODO 车辆网项目web页面可视化控制类
 */
@Slf4j
@RestController
@RequestMapping("/XTplatform")
@Api(value = "车辆可视化")
public class VehicleViewController {

    @Autowired
    private VehicleViewService vehicleViewService;

    @LogAudit
    @AutoResponse
    @ApiOperation(value = "平台车辆总数", response = Long.class, responseContainer = "Long")
    @RequestMapping(value = "vehicle/totalNum", produces = { "application/json" }, method = RequestMethod.GET)
    public Object totalNum() {
        try {
            Long totalNum = vehicleViewService.totalNum();
            return totalNum;
        } catch (Throwable t) {
            log.error("failed to sql databases.\n", t);
            return null;
        }
    }

    @LogAudit
    @AutoResponse
    @ApiOperation(value = "平台在线车辆总数", response = Long.class, responseContainer = "Long")
    @RequestMapping(value = "vehicle/onlineNum", produces = { "application/json" }, method = RequestMethod.GET)
    public Object onlineNum() {
        try {
            Long totalNum = vehicleViewService.onlineNum();
            return totalNum;
        } catch (Throwable t) {
            log.error("failed to sql databases.\n", t);
            return null;
        }
    }

    @LogAudit
    @AutoResponse
    @ApiOperation(value = "平台行驶中车辆总数", response = Long.class, responseContainer = "Integer")
    @RequestMapping(value = "vehicle/drivingNum", produces = { "application/json" }, method = RequestMethod.GET)
    public Object drivingNum() {
        try {
            Integer totalNum = vehicleViewService.drivingNum();
            return totalNum;
        } catch (Throwable t) {
            log.error("failed to sql databases.\n", t);
            return null;
        }
    }
}