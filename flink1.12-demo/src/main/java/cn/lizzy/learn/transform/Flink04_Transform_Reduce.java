package cn.lizzy.learn.transform;

import cn.lizzy.learn.source.WaterSensor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @ClassName : Flink04_Transform_Reduce
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/1 21:08
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink04_Transform_Reduce {

    public static void main(String[] args) throws Exception {
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        ExecutionEnvironment e1 = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        KeyedStream<WaterSensor, String> kbStream = env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);

        kbStream.reduce((v1, v2) -> {
            System.out.println("reduce function...");
            return new WaterSensor(v1.getId(), v1.getTs(), v1.getVc() + v2.getVc());
        }).print("reduce");


        env.execute();
    }
}
