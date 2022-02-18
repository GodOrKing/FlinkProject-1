package cn.itcast.dataservice.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 0:57
 * @Description TODO 自定义http自动响应注解
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface AutoResponse {

    /** 成功标识码 */
    String successCode() default "200";

    /** 失败标识码 */
    String errorCode() default "500";

    /** 自动识别 结果集大小 不能识别数组*/
    // boolean size() default false;

    boolean allowNull() default false;
}