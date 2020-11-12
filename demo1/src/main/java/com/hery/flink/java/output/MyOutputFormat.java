package com.hery.flink.java.output;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

/**
 * @Date 2020/7/22 14:11
 * @Created by hery
 * @Description
 * 自定义 flink 的 OutputFormat
 */
public class MyOutputFormat<T> implements OutputFormat<T> {

    private static final long serialVersionUID = 1L;

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) {
    }

    @Override
    public void writeRecord(T record) {
        // 输出信息
        System.out.println(record);
    }

    @Override
    public void close() {
    }
}