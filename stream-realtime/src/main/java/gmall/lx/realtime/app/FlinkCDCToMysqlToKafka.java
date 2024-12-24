package gmall.lx.realtime.app;

import com.stream.common.utils.KafkaUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @className: FlinkCDCToMysqlToKafka
 * @Description: TODO
 *       gmall.lx.realtime.app.FlinkCDCToMysqlToKafka
 * @author: lx      git@github.com:zhouhanker/stream-dev.git
 * bin/flink run-application -d -t yarn-application -c com.lx.Test01 /opt/dem01-1.0-SNAPSHOT-jar-with-dependencies.jar
 *
 *
 * @date: 2024/12/20 09:53
 */
public class FlinkCDCToMysqlToKafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("gmall")
                .tableList("gmall.spu_info")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> dataMysql = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdh");
        dataMysql.print();

        dataMysql.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092,cdh02:9092,cdh03:9092","topic"));
        env.execute();
    }
}
