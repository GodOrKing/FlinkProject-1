package cn.itcast.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * 自定义告警规则计算的工具类
 * 1）实现操作运算符相关方法
 *  1.1）加法
 *  1.2）减法
 *  1.3）乘法
 *  1.4）除法
 *  1.5）根据传递的操作运算符调用不同的方法
 * 2）实现规则运算符的方法
 * 3）实现逻辑运算符的方法
 * 4）自定义告警规则表达式
 */
public class CustomRuleUtil {
    //TODO 1）实现操作运算符相关方法
    // 1.1）加法
    public static double addition(double x, double y){ return x+y;}

    // 1.2）减法
    public static double subtraction(double x, double y){ return x-y;}

    // 1.3）乘法
    public static double multiplication(double x, double y){ return x*y;}

    // 1.4）除法
    public static double division(double x, double y){ return x/y;}

    /**
     * 1.5）指定操作运算符及操作的字段调用相关运算方法返回结果
     * @param x
     * @param y
     * @param z
     * @return
     */
    public static double operator(double x, double y, String z) {
        if (z.equals("+")) {
            return addition(x, y);
        } else if (z.equals("-")) {
            return subtraction(x, y);
        } else if (z.equals("*")) {
            return multiplication(x, y);
        } else if (z.equals("/")) {
            return division(x, y);
        } else {
            return x;
        }
    }

    /**
     * 2）实现规则运算符的方法
     * @param x
     * @param y
     * @param z
     * @return
     */
    public static boolean rule_symbol(double x, double y, String z){
        if(z.equals(">") && x > y){
            return true;
        }else if(z.equals(">=") && x >= y){
            return true;
        }else if(z.equals("<") && x < y){
            return true;
        }else if(z.equals("<=") && x <= y){
            return true;
        }else if(z.equals("=") && x == y){
            return true;
        }else {
            return false;
        }
    }

    /**
     * 3）实现逻辑运算符的方法
     * @return
     */
    public static boolean logical_symbol(boolean x, boolean y, String z){
        //||、&&
        if(StringUtils.equals(z, "||")){
            //先判断逻辑或
            if(x || y){
                //一个为真即为真
                return true;
            }else{
                return true;
            }
        } else if(StringUtils.equals(z, "&&")) {
            //在判断逻辑与
            if(x && y){
                //都为真才为真
                return true;
            }else{
                return false;
            }
        } else if(StringUtils.isEmpty(z) && x){
            return true;
        } else{
            return false;
        }
    }

    /**
     *
     * @param v1    参数字段1           ->         alarm_param1_field
     * @param v2    参数字段2           ->         alarm_param2_field
     * @param v3    操作符1：+ - * /    ->         operator1
     * @param v4    告警字段1的阈值      ->         alarm_threshold1
     * @param v5    参数字段1的规则符号   ->         rule_symbol1
     * @param v6    告警字段2的阈值      ->         alarm_threshold2
     * @param v7    参数字段2的规则符号   ->         rule_symbol2
     * @param v8    逻辑运算符          ->          logical_symbol
     * @return
     */
    public static boolean expression(double v1, double v2, String v3, String v4, String v5, String v6, String v7, String v8) {
        return logical_symbol(
                //将两个字段的操作符进行计算后与阈值进行判断，符合阈值条件，则为真
                rule_symbol(operator(v1, v2, v3), Double.parseDouble(v4), v5), //5 3 >   -> true
                //将两个字段的操作符进行计算后与阈值进行判断，符合阈值条件，则为真
                rule_symbol(operator(v1, v2, v3), Double.parseDouble(v6), v7), //5 8 >   -> false
                v8     //|| &&
        );
    }

    public static void main(String[] args) {
        System.out.println(CustomRuleUtil.expression(
                2, 3, "+", "3", ">",
                "8", "<", "&&"
        ));
    }
}
//
//2-4个字段
//
//        2-》两两计算
//            1 && 2
//        3-》
//            3
//        4
//            3 && 4
