package cn.itcast.dataservice.aop;

import cn.itcast.dataservice.annotation.LogAudit;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.reflect.MethodSignature;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.net.InetAddress;

/**
 * @author laowei
 * @commpany itcast
 * @Date 2020/9/16 0:53
 * @Description TODO 根据切入点获得被添加注解的方法
 *                  根据http请求获得请求客户端的ip地址
 */
@Slf4j
public abstract class AbstractAspect {
    /**
     * @desc todo 根据切点对象获得使用该注解的实例方法的名称
     * @param joinPoint
     * @return
     */
    protected String getMethodAnnotation(JoinPoint joinPoint){

        Signature signature = joinPoint.getSignature();
        String methodName = signature.getName();
        // todo 得到方法的参数的类型
        Class[] argClass = ((MethodSignature) signature).getParameterTypes();
        String annotationValue = null;
        try {
            // todo 获得当前访问的class
            Class<?> className = joinPoint.getTarget().getClass();

            // todo 得到访问的方法对象
            Method method = className.getMethod(methodName, argClass);

            // todo 判断是否存在@LogAudit注解
            if (method.isAnnotationPresent(LogAudit.class)) {
                LogAudit annotation = method.getAnnotation(LogAudit.class);
                // todo 取出注解中的方法注释
                annotationValue = annotation.value();
            }
            /** todo 判断方法是否使用的是SwagerUI对象中的ApiOperation抽象对象 */
            if(annotationValue.length() < 1 && method.isAnnotationPresent(ApiOperation.class)){
                ApiOperation annotation = method.getAnnotation(ApiOperation.class);
                // todo 取出注解中的方法注释
                annotationValue = annotation.value();
            }
        } catch (Exception e) {
            log.error(null, e);
            e.printStackTrace();
        }
        return annotationValue;
    }

    /**
     * @desc todo 根据http请求获得请求的ip地址
     * @param request
     * @return
     */
    protected String getIpAddr(HttpServletRequest request) {
        // todo todo http请求头判断，通过浏览器请求都会有header
        String ip = request.getHeader("x-forwarded-for");
        if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
            if(ip.equals("127.0.0.1") || "0:0:0:0:0:0:0:1".equals(ip)){
                // todo todo 根据网卡取本机配置的IP
                InetAddress inet=null;
                try {
                    inet = InetAddress.getLocalHost();
                } catch (Exception e) {
                    log.debug("获取本机IP失败", e);
                }
                ip= inet.getHostAddress();
            }
        }
        // todo todo 多个代理的情况，第一个IP为客户端真实IP,多个IP按照','分割
        if(ip != null && ip.length() > 15){
            if(ip.indexOf(",")>0){
                ip = ip.substring(0,ip.indexOf(","));
            }
        }
        return ip;
    }
}