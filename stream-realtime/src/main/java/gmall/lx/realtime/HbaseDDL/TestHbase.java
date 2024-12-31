package gmall.lx.realtime.HbaseDDL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @className: TestHbase
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/30 10:41
 */
public class TestHbase {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "/D:/mmmm/ideam/stream-dev-realtime/stream-realtime/src/main/resources");
        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");


        tenv.executeSql("select rk,\n" +
                "       info.login_name as login_name,\n" +
                "       info.phone_num as phone_num\n" +
                "from hbase_dim_user_info").print();
    }
}
