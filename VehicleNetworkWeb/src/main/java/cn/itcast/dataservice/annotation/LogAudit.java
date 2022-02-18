package cn.itcast.dataservice.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 0:53
 * @Description TODO 自定义日志切面类注解
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface LogAudit {
    // todo 初始化，赋默认值
    String value() default "";
}