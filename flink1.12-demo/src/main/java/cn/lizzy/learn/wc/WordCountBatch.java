package cn.lizzy.learn.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName : WordCountBatch
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/5 19:27
 * @DESCRIPTION : wordcount批处理
 * @since JDK 1.8
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. 从文件读取数据
        DataSource<String> lineDS = env.readTextFile("files/wordcount.txt");

        //3. 转换格式
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {

            String[] words = line.split(" ");

            for (String word : words) {
                collector.collect(Tuple2.of(word, 1L));
            }


        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //当lambda表达式使用的是java泛型的时候，需要声明返回的类型信息

        //4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);

        //5. 分组内聚合
        AggregateOperator<Tuple2<String, Long>> resSum = wordAndOneUG.sum(1);


        //6. 打印结果信息
        resSum.print();

    }
}
