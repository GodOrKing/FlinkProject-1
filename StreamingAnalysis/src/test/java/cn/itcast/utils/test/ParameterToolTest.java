package cn.itcast.utils.test;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class ParameterToolTest {
    public static void main(String[] args) {
        //flink字段的工具类
        ParameterTool parameterTool;

        try {
            parameterTool = ParameterTool.fromPropertiesFile(ParameterToolTest.class.getClassLoader().getResourceAsStream("conf.properties"));
            System.out.println(parameterTool.get("kafka", "test"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
