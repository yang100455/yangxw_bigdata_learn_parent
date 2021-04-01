package cn.lizzy.learn.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink01_Map_Anonymous
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/31 20:28
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink01_Map_Anonymous {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> map = env.fromElements(1, 2, 3, 4, 5)
                .map(t -> t * 2)
                //静态内部类
                .map(new MyMapFunction());

        map.print();

        env.execute();
    }

    static class MyMapFunction implements MapFunction<Integer, Integer> {

        @Override
        public Integer map(Integer value) throws Exception {
            return value - 2;
        }
    }
}
