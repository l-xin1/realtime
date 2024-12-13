package com.bw.gmall.realtime.common.until;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.Set;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

public class HBaseUtil {
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
//        conf.set("hbase.zookeeper.property.clientPort","2181");
        return ConnectionFactory.createConnection(conf);
    }

    public static void closeHBaseConn(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    public static void createHBaseTable(Connection hbaseConn,
                                        String nameSpace,
                                        String table,
                                        String... families) {
        if (families.length < 1) {
            System.out.println("至少需要一个列族");
            return;
        }
        try (Admin admin = hbaseConn.getAdmin();) {
            TableName tableName = TableName.valueOf(nameSpace, table);
            if (admin.tableExists(tableName)) {
                System.out.println("表空间" + nameSpace + "下的表" + table + "已存在");
                return;
            }
            // 列族描述器
            //表的描述器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            for (String family : families) {
                ColumnFamilyDescriptor cfDesc =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(cfDesc);// 给表设置列族
            }
            admin.createTable(tableDescriptorBuilder.build());

            System.out.println("要创建表空间" + nameSpace + "下的表" + table + "创建成功");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    //删除表
    public static void dropHBaseTable(Connection hbaseConn, String namespace, String tableName) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除表空间" + namespace + "下的表" + tableName + "不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("以删除表空间" + namespace + "下的表" + tableName + "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //向表中put数据
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> keys = jsonObj.keySet();
            for (String key : keys) {
                String value = jsonObj.getString(key);
                if (StringUtils.isNoneEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间" + namespace + "下的表" + tableName + "中的数据添加成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    //从表中删除数据
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, namespace);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间" + namespace + "下的表" + tableName + "中删除数据成功"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

}