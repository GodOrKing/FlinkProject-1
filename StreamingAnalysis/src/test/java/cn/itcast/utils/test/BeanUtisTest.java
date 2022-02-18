package cn.itcast.utils.test;

import cn.itcast.entity.RuleInfoModel;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;

public class BeanUtisTest {
    public static void main(String[] args) {
        RuleInfoModel ruleInfoModel = new RuleInfoModel();
        try {
            System.out.println(BeanUtils.getProperty(ruleInfoModel, "zhangsan"));
            System.out.println(BeanUtils.getProperty(ruleInfoModel, "ruleName"));
        } catch (Exception e) {
            System.out.println("获取属性失败");
        }
    }
}
