package cn.lizzy.learn.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @ClassName : Flink01_Source_Collection
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/31 19:54
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink01_Source_Collection {

    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensorList = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42)
        );


        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(waterSensorList).print();

        env.execute();

    }
}
