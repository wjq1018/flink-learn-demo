package com.hery.flink.java;

import com.hery.flink.java.pojo.WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Date 2020/7/2 15:43
 * @Created by hery
 * @Description
 */
public class FlinkDataType {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基本数据类型
//        DataSet<String> text = env.fromElements(WORDS);
//        DataSet<Integer> num = env.fromElements(1, 2, 3, 4);
//
        // java中的Tuple数据类型
//        DataSet<Tuple2<String, Integer>> counts =
//                // split up the lines in pairs (2-tuples) containing: (word,1)
//                text.flatMap(new BatchJob.Tokenizer())
//                        // group by the tuple field "0" and sum up tuple field "1"
//                        .groupBy(0)
//                        .sum(1);


        // pojo 类型
        DataSet<WordCount> wordCounts = env.fromElements(
                new WordCount("hello", 1),
                new WordCount("hello", 2));

//        UnsortedGrouping<WordCount> word = wordCounts.groupBy("word");
//        ReduceOperator<WordCount> reduce = word.reduce(new ReduceFunction<WordCount>() {
//            @Override
//            public WordCount reduce(WordCount w1, WordCount w2) throws Exception {
//                return new WordCount(w1.word, w1.count + w2.count);
//            }
//        });
//        reduce.print();


        // 流的方式
//        DataStream<WordCount> wcDS = senv.fromElements(
//                new WordCount("hello", 1),
//                new WordCount("hello", 2));
//        SingleOutputStreamOperator<WordCount> sum = wcDS.keyBy("word").sum("count");
//        sum.print();
//        senv.execute("FlinkDataType");

        //Row 类型

        // 获取执行环境
        // 获取table 环境
//        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
//        String path = FlinkDataType.class.getClassLoader().getResource("words.txt").getPath();
//        //  注册输入源 source
//        tEnv.connect(new FileSystem().path(path))
//                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
//                .withSchema(new Schema().field("word", Types.STRING))
//                .registerTableSource("filesource");
//        // 获取结果
//        Table result = tEnv.scan("filesource")
//                .groupBy("word")
//                .select("word,count(1) as cnt");
//        // 将结果输出打印
//        DataSet<Row> resultDS = tEnv.toDataSet(result, Row.class);
//        resultDS.print();


    }
}
