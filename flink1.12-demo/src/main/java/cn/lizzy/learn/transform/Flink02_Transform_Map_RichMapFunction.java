package cn.lizzy.learn.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink02_Transform_Map_RichMapFunction
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/31 20:47
 * @DESCRIPTION : 所有的flink 函数类都有Rich版本，他与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更加复杂的功能。
 * 也以为这提供了更多的，更丰富的功能。例如： RichMapFunction
 *
 * getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态. 开发人员在需要的时候自行调用获取运行时上下文对象.
 * @since JDK 1.8
 */
public class Flink02_Transform_Map_RichMapFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(2);

        env.fromElements(1, 2, 3, 4, 5, 6)
                .map(new MyRichMapFunction())
                .print();

        env.execute();
    }


    public static class MyRichMapFunction extends RichMapFunction<Integer, Integer> {

        // 默认生命周期方法, 初始化方法, 在每个并行度上只会被调用一次
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            RuntimeContext runtimeContext = getRuntimeContext();
            int numberOfParallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();
            String taskName = runtimeContext.getTaskName();
            System.out.println("open....." + numberOfParallelSubtasks+"," + taskName);
        }

        // 默认生命周期方法, 最后一个方法, 做一些清理工作, 在每个并行度上只调用一次
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close.....");
        }

        @Override
        public Integer map(Integer value) throws Exception {

            System.out.println("map一次");
            return value + 1;
        }
    }
}
