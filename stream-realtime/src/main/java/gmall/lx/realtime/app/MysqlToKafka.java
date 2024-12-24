package gmall.lx.realtime.app;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @className: MysqlToKafka
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/20 14:45
 */
public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        Properties properties = new Properties();
//        properties.setProperty("useSSL","false");

        MySqlSource<String> mySqlCdcSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("mar14")
                .tableList("mar14.deff")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.latest())
//                .includeSchemaChanges(true)
//                .jdbcProperties(properties)
                .build();

        DataStreamSource<String> cdcStream = env.fromSource(mySqlCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_source");

        cdcStream.print();

        cdcStream.sinkTo(
                KafkaUtils.buildKafkaSink("cdh01:9092","topic")
        );
        //.uid("sink_to_kafka_realtime_v1_mysql_db").name("sink_to_kafka_realtime_v1_mysql_db");

        env.execute();
    }
}
