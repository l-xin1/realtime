package gmall.lx.realtime.common;

import com.stream.common.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

/**
 * @className: MysqlCDCSource
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/25 19:01
 */
public class MysqlCDCSource {
    public static final String mysql_host = ConfigUtils.getString("mysql.host");
    public static final Integer mysql_port = ConfigUtils.getInt("mysql.port");

    public static MySqlSource<String> getMysqlSource(String databases, String tables,String username,String password,StartupOptions state) {
        return MySqlSource.<String>builder()
                .hostname(mysql_host)
                .port(mysql_port)
                .databaseList(databases) // set captured database
                .tableList(tables) // set captured table
                .username(username)
                .password(password)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .includeSchemaChanges(true)
                .startupOptions(state)
                .build();

    }

}
