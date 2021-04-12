package cn.lizzy.learn.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink03_Transform
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/1 20:22
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink03_Transform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<String> strStream = env.fromElements("a", "b", "c", "d", "e");
        DataStreamSource<String> strStream1 = env.fromElements("a1", "b1", "c1", "d1", "e1");

        /**
         * 滚动聚合算子
         */
        KeyedStream<Integer, String> kbStream = intStream.keyBy(e -> e % 2 == 0 ? "j" : "o");
//        kbStream.sum(1).print("sum");//按key滚动聚合
        kbStream.min(0).print("aa");
//        kbStream.max(0).print();
        //maxBy和minBy可以指定当出现相同值的时候,其他字段是否取第一个. true表示取第一个, false表示取最后一个.
        kbStream.minBy(0).print("bb");

        /**
         * union
         * 必须类型一致，可以union多个流
         */
        /*  strStream.union(strStream1).print();*/


        /**
         * connect
         * 关联两条流，仅仅是物理层面关联，其实内部还是分开的两条流
         */
       /* ConnectedStreams<Integer, String> connect = intStream.connect(strStream);
        connect.getFirstInput().print("first");
        connect.getSecondInput().print("second");*/

        env.execute();

    }
}
