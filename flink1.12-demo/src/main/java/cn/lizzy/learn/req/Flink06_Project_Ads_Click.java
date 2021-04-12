package cn.lizzy.learn.req;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink06_Project_Ads_Click
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/9 19:41
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink06_Project_Ads_Click {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.readTextFile("files/AdClickLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new AdsClickLog(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            datas[2],
                            datas[3],
                            Long.valueOf(datas[4]));
                }).map(log -> Tuple2.of(log.getProvince() + "_" + log.getAdId(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(f -> f.f0)
                .sum(1)
                .print();


        env.execute();


    }
}
