package com.hery.flink.java;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Date 2020/6/23 17:49
 * @Created by hery
 * @Description 消费kafka
 */
public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //checkpoint配置

        env.enableCheckpointing(40);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend

        //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));

        String topic = "topic-calculate-brinson";    //testKafka001
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.100.105:9092,192.168.100.106:9092,192.168.100.107:9092");
        prop.setProperty("group.id", "t1");
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        kafkaConsumer011.setStartFromEarliest();
        kafkaConsumer011.setCommitOffsetsOnCheckpoints(false);//取消提交Offset与设置检查点之间的关联。
        DataStreamSource<String> text = env.addSource(kafkaConsumer011);
        text.print();
        env.execute("kafkaDemo");

    }
}
