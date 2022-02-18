package cn.itcast.dataservice.aop;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 0:50
 * @Description TODO 动态数据源切换实现类
 */
public class DynamicDataSource extends AbstractRoutingDataSource {

    // todo 定义一个本地的线程
    private static final ThreadLocal<String> dataSources = new InheritableThreadLocal<>();

    // todo 设置数据源对象
    public static void setDataSource(String dataSource) {
        dataSources.set(dataSource);
    }

    public static void clearDataSource() {
        dataSources.remove();
    }

    // todo 检查数据绑定的数据源上下文信息
    @Override
    protected Object determineCurrentLookupKey() {
        return dataSources.get();
    }

}
