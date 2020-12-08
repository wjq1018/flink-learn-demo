package yss.dm.sinkFunction;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import yss.dm.common.ConstantInfo;
import yss.dm.mysqlSouce.Student;

import java.util.Properties;

/**
 * @ClassName KafkaUtils2
 * @Description //往kafka中写数据,可以使用这个main函数进行测试一下
 * @Date  2020/12/8 13:42
 * @Author wjq
 **/
public class KafkaUtils2 {
    public static final String broker_list = ConstantInfo.BROKER_LIST;
    public static final String topic = "student";  //kafka topic 需要和 flink 程序用同一个 topic

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "student-name" + i, "stu-password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}