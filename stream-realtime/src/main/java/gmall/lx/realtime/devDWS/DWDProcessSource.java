package gmall.lx.realtime.devDWS;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @className: DWDProcessSource
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/27 18:50
 */
public class DWDProcessSource {

    public static void newTable(StreamTableEnvironment tEnv){
         tEnv.executeSql("CREATE TABLE gmallTable (\n" +
                "  `after` map<string,string>,\n" +
                "  `source` map<string,string>,\n" +
                "  `op` string ,\n" +
                "   `ts_ms` bigint,\n" +
                "  times AS TO_TIMESTAMP(FROM_UNIXTIME(ts_ms/1000))," +
                " WATERMARK FOR times AS times - INTERVAL '0' SECOND " +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
    }
    public static Table getFavorAdd(StreamTableEnvironment tEnv){
        DWDProcessSource.newTable(tEnv);
        return tEnv.sqlQuery(" select after['id'] id,after['user_id'] user_id," +
                " after['sku_id'] sku_id,TO_TIMESTAMP(FROM_UNIXTIME(CAST(after['create_time'] AS BIGINT)/1000)) create_time "+
                " from gmallTable  where source['table']='favor_info' ");
//        tEnv.createTemporaryView("dwd_favor_add",dwd_favor_info);
//        tEnv.sqlQuery("select * from dwd_favor_add").execute().print();
    }
    public static Table getCouponUse(StreamTableEnvironment tEnv){
        DWDProcessSource.newTable(tEnv);
        return tEnv.sqlQuery(" select after['id'] id, after['coupon_id'] coupon_id,after['user_id'] user_id," +
                " after['order_id'] order_id,after['using_time'] using_time,after['used_time'] used_time," +
                " after['coupon_status'] coupon_status "+
                " from gmallTable  where source['table']='coupon_use' ");

    }
    public static Table getCouponGet(StreamTableEnvironment tEnv) {
        DWDProcessSource.newTable(tEnv);
        return tEnv.sqlQuery(" select after['id'] id, after['coupon_id'] coupon_id,after['user_id'] user_id," +
                        "  TO_TIMESTAMP(FROM_UNIXTIME(CAST(after['get_time'] AS BIGINT)/1000)) get_time,after['coupon_status'] coupon_status "+
                " from gmallTable  where source['table']='coupon_use' ");
    }
    public static Table getUserInfo(StreamTableEnvironment tEnv) {
        DWDProcessSource.newTable(tEnv);
        return tEnv.sqlQuery(" select after['id'] id," +
                " TO_TIMESTAMP(FROM_UNIXTIME(CAST(after['create_time'] AS BIGINT)/1000)) create_time "+
                " from gmallTable  where source['table']='user_info' ");
    }


}
