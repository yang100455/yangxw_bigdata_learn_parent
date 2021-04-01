package cn.lizzy.learn.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink02_Source_File
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/31 19:57
 * @DESCRIPTION :
 * @since JDK 1.8
 */
public class Flink02_Source_File {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.可以读取文件，也可以读取目录
        DataStreamSource<String> source = env.readTextFile("/Users/apple/data/workspace/workspace_learn/yangxw-bigdata-learn-parent/files");

        //3.
        source.print();

        env.execute();
    }
}
