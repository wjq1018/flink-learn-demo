package TableApi.explain;

import org.apache.flink.table.api.*;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName TableApi.explain.TestTableApi2
 * @Description: //使用StatementSet.TableApi.explain()方法的多槽计划的相应输出
 * @Date Create in 2020/11/13
 * @Author wjq
 */
public class TestTableApi2 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        final Schema schema = new Schema()
                .field("count", DataTypes.INT())
                .field("word", DataTypes.STRING());

        tEnv.connect(new FileSystem().path("/source/path1"))
                .withFormat(new Csv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySource1");
        tEnv.connect(new FileSystem().path("/source/path2"))
                .withFormat(new Csv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySource2");
        tEnv.connect(new FileSystem().path("/sink/path1"))
                .withFormat(new Csv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySink1");
        tEnv.connect(new FileSystem().path("/sink/path2"))
                .withFormat(new Csv().deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("MySink2");

        StatementSet stmtSet = tEnv.createStatementSet();

        Table table1 = tEnv.from("MySource1").where($("word").like("F%"));
        stmtSet.addInsert("MySink1", table1);

        Table table2 = table1.unionAll(tEnv.from("MySource2"));
        stmtSet.addInsert("MySink2", table2);

        String explanation = stmtSet.explain();
        System.out.println(explanation);
    }
}

