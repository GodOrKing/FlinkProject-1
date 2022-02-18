package cn.itcast.dataservice.config.druid.mysql;

import cn.itcast.dataservice.config.druid.DataSourceCommonProperties;
import cn.itcast.dataservice.config.druid.DataSourceProperties;
import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 1:06
 * @Description TODO Mysql配置信息对应的对象
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({DataSourceProperties.class, DataSourceCommonProperties.class})
public class MysqlConfig {

    @Autowired
    private DataSourceProperties dataSourceProperties;

    @Autowired
    private DataSourceCommonProperties dataSourceCommonProperties;

    /**
     * @desc todo 根据mysql配置信息获得dataSource对象
     * @return DataSource(mysqlDataSource)
     */
    @Primary
    @Bean("mysqlDataSource")
    @Qualifier("mysqlDataSource")
    public DataSource dataSource(){
        DruidDataSource datasource = new DruidDataSource();

        // todo 配置数据源属性
        datasource.setUrl(dataSourceProperties.getMysql().get("url"));
        datasource.setUsername(dataSourceProperties.getMysql().get("username"));
        datasource.setPassword(dataSourceProperties.getMysql().get("password"));
        datasource.setDriverClassName(dataSourceProperties.getMysql().get("driver-class-name"));

        // todo 配置统一属性
        datasource.setInitialSize(dataSourceCommonProperties.getInitialSize());
        datasource.setMinIdle(dataSourceCommonProperties.getMinIdle());
        datasource.setMaxActive(dataSourceCommonProperties.getMaxActive());
        datasource.setMaxWait(dataSourceCommonProperties.getMaxWait());
        datasource.setTimeBetweenEvictionRunsMillis(dataSourceCommonProperties.getTimeBetweenEvictionRunsMillis());
        datasource.setMinEvictableIdleTimeMillis(dataSourceCommonProperties.getMinEvictableIdleTimeMillis());
        datasource.setValidationQuery(dataSourceCommonProperties.getValidationQuery());
        datasource.setTestWhileIdle(dataSourceCommonProperties.isTestWhileIdle());
        datasource.setTestOnBorrow(dataSourceCommonProperties.isTestOnBorrow());
        datasource.setTestOnReturn(dataSourceCommonProperties.isTestOnReturn());
        datasource.setPoolPreparedStatements(dataSourceCommonProperties.isPoolPreparedStatements());
        try {
            datasource.setFilters(dataSourceCommonProperties.getFilters());
        } catch (SQLException e) {
            log.error("Druid configuration initialization filter error.", e);
        }
        return datasource;
    }
}
