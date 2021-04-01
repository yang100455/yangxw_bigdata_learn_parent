package cn.lizzy.learn;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName : cdc
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/25 14:09
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class MySqlBinlogSourceExample {

    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("172.17.9.90")
                .port(3306)
                .databaseList("test")
                .username("private_cloud")
                .password("csc_stats")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        System.setProperty("HADOOP_USER_NAME", "yangxw");
        env.setStateBackend(new FsStateBackend("hdfs://nameserviceQA:8020/user/yangxw"));

        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointInterval(1000);

        DataStreamSource<String> dsSource = env.addSource(sourceFunction);
        dsSource.print().setParallelism(1);

        env.execute();
    }
}
