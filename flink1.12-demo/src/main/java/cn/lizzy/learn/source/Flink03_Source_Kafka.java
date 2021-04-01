package cn.lizzy.learn.source;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName : Flink03_Source_Kafka
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/31 20:00
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink03_Source_Kafka {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.17.9.94:9092,172.17.9.98:9092");
        properties.setProperty("group.id", "Flink03_Source_Kafka");

        //earliest
        //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //latest
        //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        properties.setProperty("auto.offset.reset", "latest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new FlinkKafkaConsumer<>("flink_test", new SimpleStringSchema(), properties))
                .print("kafka source");

        env.execute();
    }
}
