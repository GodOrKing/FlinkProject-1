package cn.itcast.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置文件读取的工具类
 * 加载配置文件，然后加载key对应的value值
 */
public class ConfigLoader {
    /**
     * 开发步骤：
     * 1：使用classLoader加载类对象，然后加载conf.properties
     * 2：使用Properties的load方法加载inputStream
     * 3：编写方法获取配置项的key对应的value值
     * 4：编写方法获取int类型的key对应的value值
     */

    //TODO 1：使用classLoader加载类对象，然后加载conf.properties
    private final static InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream("conf.properties");

    //TODO 2：使用Properties的load方法加载inputStream
    private final static Properties props = new Properties();
    static {
        try {
            //加载inputStream-》conf.properties
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //TODO 3：编写方法获取配置项的key对应的value值
    public static String getProperty(String key) { return  props.getProperty(key); }

    //TODO 4：编写方法获取int类型的key对应的value值
    public static Integer getInteger(String key)  { return Integer.parseInt(props.getProperty(key)); }
}
