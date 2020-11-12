package com.hery.flink.java;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Date 2020/6/29 18:17
 * @Created by hery
 * @Description
 */
public class Tmp {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = null;
        text = env.fromElements("hello,you,hello,me");

        DataStream<String> words = text.map(new RichMapFunction<String, String>() {


            ExecutionEnvironment benv = null;

            public void open(Configuration parameters) throws Exception {

                ExecutionConfig conf = getRuntimeContext().getExecutionConfig();

                benv = ExecutionEnvironment.getExecutionEnvironment();
                super.open(parameters);
            }

            @Override
            public String map(String s) throws Exception {
                DataSet<String> text = benv.fromElements("on be, or not to be,--that is the question:");
                DataSet<Tuple2<String, Integer>> counts =
                        // split up the lines in pairs (2-tuples) containing: (word,1)
                        text.flatMap(new BatchJob.Tokenizer())
                                // group by the tuple field "0" and sum up tuple field "1"
                                .groupBy(0)
                                .sum(1);
                counts.print();
                return "111...";
            }
        });


        words.print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
