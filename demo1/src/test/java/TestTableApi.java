import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName TestTableApi
 * @Description: 测试使用 Flink_Table_Api
 * @Date Create in 2020/11/12
 * @Author wjq
 */
public class TestTableApi {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

        // explain Table API
        Table table1 = tEnv.fromDataStream(stream1, $("count"), $("word"));
        Table table2 = tEnv.fromDataStream(stream2, $("count"), $("word"));
        Table table = table1
                .where($("word").like("F%"))
                .unionAll(table2);
        System.out.println(table.explain());

    }
}
