package cn.itcast.dataservice.aop;

import cn.itcast.dataservice.annotation.DataSource;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 0:51
 * @Description TODO 定义动态数据源切面实现类
 *               @Aspect：定义切面类注解
 *               @Component：当前类创建对象，交给spring管理
 */
@Aspect
@Component
public class DynamicDataSourceAspect {

    private final Logger logger = LoggerFactory.getLogger(DynamicDataSourceAspect.class);
    // todo 配置切入点
    @Pointcut("@annotation(cn.itcast.dataservice.annotation.DataSource)")
    public void annotationPointCut() { }
    // todo 进入切入点后，执行通知方法，最先执行此方法
    @Before("annotationPointCut()")
    public void beforeSwitchDataSource(JoinPoint point) {
        // todo 获得切入点实例对象
        Signature signature = point.getSignature();
        // todo 获得访问的方法名
        String methodName = signature.getName();
        // todo 得到方法的参数的类型
        Class[] argClass = ((MethodSignature) signature).getParameterTypes();
        // todo 数据名称
        String dataSource = null;
        try {
            // todo 获得当前访问的class
            Class<?> className = point.getTarget().getClass();

            // todo 得到访问的方法对象
            Method method = className.getMethod(methodName, argClass);

            // todo 判断是否存在@DataSource注解
            if (method.isAnnotationPresent(DataSource.class)) {
                DataSource annotation = method.getAnnotation(DataSource.class);
                // todo 取出注解中的数据源名
                dataSource = annotation.value();
            }
        } catch (Exception e) {
            logger.error(null, e);
            e.printStackTrace();
        }

        // todo 切换数据源
        DynamicDataSource.setDataSource(dataSource);

    }

    // todo 出切入点之前，最后执行此方法
    @After("annotationPointCut()")
    public void afterSwitchDataSource(JoinPoint point) {
        DynamicDataSource.clearDataSource();
    }
}