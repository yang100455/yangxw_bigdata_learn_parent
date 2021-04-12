package cn.lizzy.learn.req;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName : Flink01_Project_PV
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/8 20:39
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink01_Project_PV2 {

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
             .keyBy(b->b.getBehavior())
                .process(new ProcessFunction<UserBehavior, Long>() {
                    Long count = 0L;
                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        count++;
                        out.collect(count);
                    }
                }).print();

        env.execute();

    }
}
