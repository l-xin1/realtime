package gmall.lx.realtime.test;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @className: TradeTest
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/31 14:48
 */
public class TradeTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);




        env.execute();
    }
}
