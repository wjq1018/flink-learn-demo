package yss.dm.mysqlSouce;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;


/**
 * @ClassName Main2
 * @Description //测试 mysql作为Source
 * @Date  2020/12/8 13:42
 * @Author wjq
 **/
public class Main2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.addSource(new SourceFromMySQL()).print();
        DataStreamSource<Student> source = env.addSource(new SourceFromMySQL());
        source.addSink(new PrintSinkFunction<>());

        env.execute("Flink add mysql data sourc");
    }
}