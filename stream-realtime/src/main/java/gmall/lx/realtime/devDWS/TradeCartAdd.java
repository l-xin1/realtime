package gmall.lx.realtime.devDWS;

import gmall.lx.realtime.common.KafkaNewUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @className: TradeCartAdd
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/27 14:05
 */
public class TradeCartAdd {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table favorAdd = DWDProcessSource.getFavorAdd(tEnv);
        tEnv.createTemporaryView("favor_add",favorAdd);
        Table table = tEnv.sqlQuery(" select * from favor_add");

        DataStream<Row> rowDataStream = tEnv.toDataStream(table);
        System.out.println(rowDataStream.toString());
        env.close();
    }
}
