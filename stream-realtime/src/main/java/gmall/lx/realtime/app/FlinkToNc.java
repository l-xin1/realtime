package gmall.lx.realtime.app;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/**
 * @className: FlinkToNc
 * @Description: TODO
 *       gmall.lx.realtime.app.FlinkToNc
 * @author: lx
 * @date: 2024/12/20 09:46
 */
public class FlinkToNc {
    @SneakyThrows
    public static void main(String[] args)  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("cdh02", 14777);
        dataStreamSource.print();
        env.execute();
    }
}
