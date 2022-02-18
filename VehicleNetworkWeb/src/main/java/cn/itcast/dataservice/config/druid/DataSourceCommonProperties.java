package cn.itcast.dataservice.config.druid;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:04
 * @Description TODO 创建springboot中关于数据源的公共属性映射对象：第四级对象
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "spring.datasource.commonconfig", ignoreUnknownFields = false)
public class DataSourceCommonProperties {

    private int initialSize = 10;
    private int minIdle;
    private int maxActive;
    private int maxWait;
    private int timeBetweenEvictionRunsMillis;
    private int minEvictableIdleTimeMillis;
    private String validationQuery;
    private boolean testWhileIdle;
    private boolean testOnBorrow;
    private boolean testOnReturn;
    private boolean poolPreparedStatements;
    private int maxOpenPreparedStatements;
    private String filters;
    private String mapperLocations;

}