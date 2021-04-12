package cn.lizzy.learn.req;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink01_Project_PV
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/8 20:39
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink01_Project_PV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> fileDS = env.readTextFile("files/UserBehavior.csv");

        fileDS.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(
                    Long.valueOf(split[0]),
                    Long.valueOf(split[1]),
                    Integer.valueOf(split[2]),
                    split[3],
                    Long.valueOf(split[4]));
        }).filter(behavior -> "pv".equalsIgnoreCase(behavior.getBehavior()))
                .map(behavior -> Tuple2.of("pv", 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(v -> v.f0)
                .sum(1)
                .print();

        env.execute();

    }
}
