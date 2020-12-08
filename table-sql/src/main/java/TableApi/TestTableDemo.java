//package TableApi;
//
//
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//import static org.apache.flink.table.api.Expressions.$;
//
///**
// * @ClassName TestTableDemo
// * @Description: //测试Table
// * @Date Create in 2020/11/18
// * @Author wjq
// */
//public class TestTableDemo {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
//
//        DataStream<TestOrder> TableA = bsEnv.fromCollection(
//                Arrays.asList(new TestOrder("a", "pro01", 1),
//                        new TestOrder("b", "pro02", 2)));
//
//        bsTableEnv.createTemporaryView("TableA", TableA, $("user"), $("product"), $("amount"));
////        bsTableEnv.sqlQuery("select * from TableA").execute().print();
//        bsTableEnv.executeSql("select * from TableA").print();
////        bsTableEnv.execute("test");
////        bsEnv.execute();
//
//    }
//
//    @Data
//    @AllArgsConstructor
//    @NoArgsConstructor
//    public static class TestOrder {
//        public String user;
//        public String product;
//        public int amount;
//    }
//}
//
