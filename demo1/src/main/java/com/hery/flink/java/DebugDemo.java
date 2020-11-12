package com.hery.flink.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Date 2020/7/2 11:45
 * @Created by hery
 * @Description
 */
public class DebugDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = null;
        text = env.fromElements("hello,you,hello,me");
        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new StreamingJob.Tokenizer()).map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2;
                    }
                })
                        .keyBy(0)
                        .sum(1);

        System.out.println("并行度：" + env.getParallelism());
        env.execute("Flink StreamingJob");
    }
}
