package cn.itcast.dataservice.config.swagger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:25
 * @Description TODO Swagger-UI启动主配置类，提供接口API展示页面
 */
@Configuration
@EnableSwagger2
public class SwaggerConfig {
    private ApiInfo apiInfo() {
        // todo 创建API构建对象，并设置对象相关属性
        return new ApiInfoBuilder()
                .title("分析平台-数据接口Restful API文档")
                .description("本文档提供并展示系统所有对外接口的明细")
                .termsOfServiceUrl("")
                .version("2.0")
                .contact(new Contact("传智-黑马程序员-车联网大数据", "http://www.itcast.cn/", "heima@itcast.cn"))
                .build();
    }

    /**
     * @desc todo 设置自定义实现数据服务接口，指定api使用的springmvc请求的controller(选择包路径下所有的controller对象)
     * @return todo 接口API构建器，是Springfox框架的主要接口，为后台数据服务接口提供合理的默认值和便捷的访问方法。
     */
    @Bean
    public Docket customImplementation(){
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("cn.itcast.dataservice.controller"))
                .build();
    }
}