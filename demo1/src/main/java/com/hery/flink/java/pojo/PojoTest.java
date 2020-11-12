package com.hery.flink.java.pojo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * @Date 2020/7/8 13:57
 * @Created by hery
 * @Description
 */
public class PojoTest {
    public static void main(String[] args) {
        // 检测对象是否为pojo
        System.out.println(TypeInformation.of(WordCount.class).createSerializer(new ExecutionConfig()));

        System.out.println(TypeInformation.of(StockPrice.class).createSerializer(new ExecutionConfig()));
    }
}
