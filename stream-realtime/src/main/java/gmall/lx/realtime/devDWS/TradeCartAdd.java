package gmall.lx.realtime.devDWS;

import gmall.lx.realtime.common.KafkaNewUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @className: TradeCartAdd
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/27 14:05
 */
public class TradeCartAdd {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

         DWDProcessSource.newTable(tEnv);

         //加购表
        Table cart = tEnv.sqlQuery(" select after['id'] id," +
                "after['user_id'] user_id," +
                "after['sku_id'] sku_id," +
                "after['sku_num'] sku_num," +
                "TO_TIMESTAMP(FROM_UNIXTIME(CAST(after['order_time'] AS BIGINT)/1000)) order_time, " +
                " times " +
                "from gmallTable " +
                " where source['table']='cart_info' and  cast(after['is_ordered'] as int) > 0 ");
        tEnv.executeSql(" create table dwd_trade_cart_add ( " +
                "id string,user_id string,sku_id string," +
                "sku_num string,order_time TIMESTAMP(3),times TIMESTAMP(3)) with(" +
                " 'connector'='kafka'," +
                " 'topic'='dwd_trade_cart_add'," +
                " 'properties.bootstrap.servers'='cdh01:9092'," +
                " 'format'='json' " +
                ") ");
        cart.executeInsert("dwd_trade_cart_add");



        env.close();
    }
}
