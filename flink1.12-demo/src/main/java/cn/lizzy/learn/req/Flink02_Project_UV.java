package cn.lizzy.learn.req;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @ClassName : Flink01_Project_PV
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/8 20:39
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink02_Project_UV {

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
                .map(b -> Tuple2.of("uv", b.getUserId()))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {

                    HashSet<Long> userIds = new HashSet();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        userIds.add(value.f1);
                        out.collect(userIds.size());
                    }
                })
                .print();

        env.execute();

    }
}
