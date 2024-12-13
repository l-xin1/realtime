package com.bw.gmall.realtime.common.until;

import com.bw.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 从头消费
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {

                                if (bytes != null) {
                                    return new String(bytes);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                // 从最末尾开始消费
//                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        return kafkaSource;
    }

    public static MySqlSource<String> getMysqlSource(String database, String tableName) {
        Properties props = new Properties();
        props.setProperty("userSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(database) // set captured database
                .tableList(database + "." + tableName) // set captured table
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                // 设置全量读取
                .startupOptions(StartupOptions.initial())
//                .startupOptions(StartupOptions.latest())
                .build();
        return mySqlSource;
    }
}
