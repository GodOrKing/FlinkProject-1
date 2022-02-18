package cn.itcast.dataservice.config.druid;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:03
 * @Description TODO 存放DataSource属性对象，第三级对象
 *                  @ConfigurationProperties： 这个注解配置之后，意味着会自定去springboot的默认文件
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "spring.datasource", ignoreUnknownFields = false)
public class DataSourceProperties {
    private Map<String,String> mysql;
    private Map<String,String> hive;
    private Map<String,String> commonconfig;
}