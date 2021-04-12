package cn.lizzy.learn.req;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @ClassName : Flink04_Project_Analysis_By_Channel
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/9 10:15
 * @DESCRIPTION : 一般会通过各种不同的渠道对自己的APP进行市场推广，而这些渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）就成了市场营销的重要商业指标。
 * @since JDK 1.8
 */
public class Flink04_Project_Analysis_By_Channel {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<String, Long>> tmp = env.addSource(new AppMarketingDataSource())
                .map(b -> Tuple2.of(b.getChannel() + "_" + b.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG)).setParallelism(2);

        System.out.println(tmp.getParallelism()+"<<<<<<<1");

        KeyedStream<Tuple2<String, Long>, String> tmp2 = tmp
                .keyBy(t -> t.f0);

        System.out.println(tmp2.getParallelism()+"<<<<<<<2");
        tmp2
                .sum(1)
                .print();

        env.execute();
    }



    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {

        boolean isCancel = false;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

            while (!isCancel) {
                MarketingUserBehavior s = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());

                ctx.collect(s);

                Thread.sleep(2000);
            }

        }

        @Override
        public void cancel() {

            isCancel = true;
        }
    }
}


