package gmall.lx.realtime.HbaseDDL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @className: TestHbaseDDL
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/30 10:43
 */
public class TestHbaseDDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        String createHiveCatalogDDL = "create catalog hive_catalog with (\n" +
                "    'type'='hive',                                      \n" +
                "    'default-database'='default',                       \n" +
                "    'hive-conf-dir'='/D:/mmmm/ideam/stream-dev-realtime/stream-realtime/src/main/resources'\n" +
                ")";
        //D:\mmmm\ideam\stream-dev-realtime\stream-realtime\src\main\resources

        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "/D:/mmmm/ideam/stream-dev-realtime/stream-realtime/src/main/resources");
        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");
        tenv.executeSql(createHiveCatalogDDL).print();
    }
}
