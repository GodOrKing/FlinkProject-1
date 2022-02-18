package cn.itcast.dataservice;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 0:48
 * @Description TODO 后台数据服务SpringBoot启动类
 *  @SpringBootApplication: 配置springboot快速启动类注解
 *  @EnableSwagger2 ： 接口开发工具
 *  @MapperScan ： 扫描加载mybatis的接口的包路径
 *
 */
@SpringBootApplication
@EnableSwagger2
@MapperScan("cn.itcast.dataservice.mapper")
public class DataApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataApplication.class, args);
    }

}