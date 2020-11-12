package com.hery.flink.java.api;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @Date 2020/8/19 11:09
 * @Created by hery
 * @Description
 */
public class DataStreamAPISourceJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Path pa = new Path("D:\\hery\\flink_learning\\code\\flink_study\\src\\main\\resources\\tmpData");
        // 定义format信息
        TextInputFormat format = new TextInputFormat(pa);
        format.setCharsetName("UTF-8");

        // 定义typyInfo 信息
        BasicTypeInfo typeInfo = BasicTypeInfo.STRING_TYPE_INFO;

        DataStream<String> st = env.readFile(format, "D:\\hery\\flink_learning\\code\\flink_study\\src\\main\\resources\\tmpData");
        /**
         * PROCESS_CONTINUOUSLY 增量处理，定期扫描路径，来查找新数据
         * PROCESS_ONCE 处理一次，处理路径下当前的内容并最后退出
         */
        DataStream<String> st1 = env.readFile(format,
                "/home/master/qingshu",
                FileProcessingMode.PROCESS_CONTINUOUSLY,
                1L,
                (TypeInformation) typeInfo);

        // 输出
        st.print();

        env.execute("DataStreamAPISourceJava");
    }
}
