package gmall.lx.realtime.devDWS;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @className: TradeOrderDetail
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/28 11:19
 */
public class TradeOrderDetail {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        DWDProcessSource.newTable(tEnv);
//        Table table = tEnv.sqlQuery(" select after['id'] id,after['order_id'] order_id," +
//                " after['sku_id'] sku_id,after['sku_name'] sku_name," +
//                " after['create_time'] create_time," +
//                " after['sku_num'] sku_num," +
//                " after['split_total_amount'] split_total_amount," +
//                " after['split_activity_amount'] split_activity_amount," +
//                " after['split_coupon_amount'] split_coupon_amount," +
//                " times " +
//                " from  gmallTable " +
//                "where source['table']='order_detail' and source['db']='gmall' ");
//        table.execute().print();

//        tEnv.executeSql("CREATE TABLE orderDetail (\n" +
//                "  id BIGINT,\n" +
//                "  order_id BIGINT,\n" +
//                "  sku_id BIGINT,\n" +
//                "  sku_name String,\n" +
//                "  create_time String," +
//                "  split_total_amount String," +
//                "  split_activity_amount String, \n" +
//                "  split_coupon_amount String " +
//                ") WITH (\n" +
//                "   'connector'='jdbc',\n" +
//                "   'driver'='com.mysql.cj.jdbc.Driver'," +
//                "   'url' = 'jdbc:mysql://cdh03:3306/gmall',\n" +
//                "   'table-name' = 'order_detail'," +
//                "  'username'='root' ," +
//                "  'password'='root' " +
//                ")");
//
//        tEnv.sqlQuery(" select * from orderDetail ").execute().print();

        env.execute();
    }
}
