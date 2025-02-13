package gmall.lx.realtime.test;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import gmall.lx.realtime.common.JsonDeserializationSchemaUtil;
import gmall.lx.realtime.common.KafkaNewUtil;
import gmall.lx.realtime.common.MyDeserializationSchemaFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
                .tableList("gmall.") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setParallelism(1);
//        DataStreamSource<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//        SingleOutputStreamOperator<String> streamOperator = source.filter(m -> JSONObject.parseObject(m).getJSONObject("source").getString("table").equals("order_detail"));
//        streamOperator.print();
//        source.print();
//        source.sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092","topic_db"));
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        SingleOutputStreamOperator<String> streamOperator = kafkaSource.filter(m -> JSONObject.parseObject(m).getJSONObject("source").getString("table").equals("order_detail"));
        kafkaSource.print();


        env.execute();
    }
}
