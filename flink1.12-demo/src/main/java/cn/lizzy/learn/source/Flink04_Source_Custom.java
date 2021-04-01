package cn.lizzy.learn.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink04_Source_Custom
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/31 20:24
 * @DESCRIPTION : 自定义source
 * @since JDK 1.8
 */
public class Flink04_Source_Custom {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MySource("localhost", 1111)).print();

        env.execute();

    }
}
