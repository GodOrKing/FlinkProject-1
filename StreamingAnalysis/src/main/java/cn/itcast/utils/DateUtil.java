package cn.itcast.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间处理工具类
 * 需要实现如下方法：
 * 1）直接获取当前时间，格式：“yyyy-MM-dd HH:mm:ss”
 * 2）直接获取当前日期，格式：“yyyyMMdd”
 * 3）字符串日期转换方法，传入参数格式“yyyy-MM-dd HH:mm:ss”，转换成Date类型
 * 4）字段串日期转换方法，传入参数格式“yyyy-MM-dd HH:mm:ss”，转换成“yyyyMMdd”格式
 */
public class DateUtil {
    /**
     * 1）直接获取当前时间，格式：“yyyy-MM-dd HH:mm:ss”
     */
    public static String getCurrentDateTime() {
        return new SimpleDateFormat(DateFormatDefine.DATE_TIME_FORMAT.getFormat()).format(new Date());
    }

    /**
     * 2）直接获取当前日期，格式：“yyyyMMdd”
     */
    public static String getCurrentDate(){
        return new SimpleDateFormat(DateFormatDefine.DATE_FORMAT.getFormat()).format(new Date());
    }

    /**
     * 3）字符串日期转换方法，传入参数格式“yyyy-MM-dd HH:mm:ss”，转换成Date类型
     * @param str
     * @return
     */
    public static Date convertStringToDate(String str){
        Date date = null;
        try {
            //注意：SimpleDateFormat是线程非安全的，因此使用的时候每次都必须要创建一个实例
            SimpleDateFormat format = new SimpleDateFormat(DateFormatDefine.DATE_TIME_FORMAT.getFormat());
            date = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public static Date convertDateStrToDate(String str){
        Date date = null;
        try {
            //注意：SimpleDateFormat是线程非安全的，因此使用的时候每次都必须要创建一个实例
            SimpleDateFormat format = new SimpleDateFormat(DateFormatDefine.DATE2_FORMAT.getFormat());
            date = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 4）字段串日期转换方法，传入参数格式“yyyy-MM-dd HH:mm:ss”，转换成“yyyyMMdd”格式
     * @param str
     * @return
     */
    public static String convertStringToDateString(String str){
        String dateStr = null;
        try {
            //1：将传入的字符串转换成日期对象对象
            Date date = new SimpleDateFormat(DateFormatDefine.DATE_TIME_FORMAT.getFormat()).parse(str);
            //2：将转换后的日期对象转换成指定的日期字符串
            dateStr = new SimpleDateFormat(DateFormatDefine.DATE_FORMAT.getFormat()).format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dateStr;
    }

}
