package gmall.lx.realtime.HbaseDDL;

//import com.alibaba.fastjson.JSONObject;
//import lombok.SneakyThrows;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.CellUtil;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.util.ArrayList;
//import java.util.Iterator;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @className: TestConnect2HBase
 * @Description: TODO
 * @author: lx
 * @date: 2025/1/2 14:01
 */
public class TestConnect2HBase {

    @SneakyThrows
    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "cdh02");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(hbaseConf);
        Table HbaseTable = connection.getTable(TableName.valueOf("gmall:dim_user_info"));
        Scan scan = new Scan();
////        scan.setLimit(Math.toIntExact(limit));
        ResultScanner scanner = HbaseTable.getScanner(scan);

        for (Result result : scanner) {
            // 打印行键
            // 假设列族名为 "cf"，列名为 "column"，根据实际情况修改
//            byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("create_time"));
                System.out.println("Row Key: " + Bytes.toString(result.getRow())+" , Value: " + result.value());
        }
            scanner.close();
            HbaseTable.close();
            connection.close();
//        // 获取表对象
//        Table table = connection.getTable(TableName.valueOf("gmall:dim_user_info"));
//
//        // 读取单行数据
//        Get get = new Get(Bytes.toBytes("gmall:dim_user_info"));
//        Result result = table.get(get);
//
//        // 处理结果
//        for (Cell cell : result.rawCells()) {
//            String row = Bytes.toString(CellUtil.cloneRow(cell));
//            String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
//            String columnQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
//            String value = Bytes.toString(CellUtil.cloneValue(cell));
//            System.out.println("Row: " + row + ", Column Family: " + columnFamily + ", Column Qualifier: " + columnQualifier + ", Value: " + value);
//        }
//
//        // 读取多行数据
//        Scan scan = new Scan();
//        ResultScanner scanner = table.getScanner(scan);
//        for (Result scanResult : scanner) {
//            for (Cell scanCell : scanResult.rawCells()) {
//                String scanRow = Bytes.toString(CellUtil.cloneRow(scanCell));
//                String scanColumnFamily = Bytes.toString(CellUtil.cloneFamily(scanCell));
//                String scanColumnQualifier = Bytes.toString(CellUtil.cloneQualifier(scanCell));
//                String scanValue = Bytes.toString(CellUtil.cloneValue(scanCell));
//                System.out.println("Row: " + scanRow + ", Column Family: " + scanColumnFamily + ", Column Qualifier: " + scanColumnQualifier + ", Value: " + scanValue);
//            }
//        }
//
//        // 关闭资源
//        scanner.close();
//        table.close();
//        connection.close();


//        env.execute();

    }
}