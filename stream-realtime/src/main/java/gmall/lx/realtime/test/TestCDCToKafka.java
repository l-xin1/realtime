package gmall.lx.realtime.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import gmall.lx.realtime.common.KafkaNewUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @className: TestCDCToKafka
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/27 09:57
 */
public class TestCDCToKafka {
    @SneakyThrows
    public static void main(String[] args) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("gmall") // set captured database
                .tableList("gmall.*") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setParallelism(1);
        DataStreamSource<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        source.print();
//        source.sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092","topic_db"));

        env.execute();
    }
}
