package cn.lizzy.learn.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @ClassName : MySource
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/31 20:15
 * @DESCRIPTION : 实现从一个socket读取数据的source
 * @since JDK 1.8
 */
public class MySource implements SourceFunction<WaterSensor> {

    private String host;
    private int port;
    private volatile boolean isRunning = true;
    private Socket socket;


    public MySource(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {

        socket = new Socket(host, port);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

        String line;
        while (isRunning && (line = reader.readLine()) != null) {
            String[] split = line.split(",");
            ctx.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
