package gmall.lx.realtime.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @className: MysqlToTest
 * @Description: TODO
 * @author: lx
 * @date: 2025/1/8 15:10
 */
public class MysqlToTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.10.113")
                .port(3306)
                .databaseList("test01") // set captured database
                .tableList("test01.product") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.latest())
                .build();
        env.enableCheckpointing(300);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL-Source")
                .print();

        env.execute();

    }
}
