package com.bw.gmall.realtime.common.until;


import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.List;
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
    /**
     * 根据参数从 hbase 指定的表中查询一行数据
     *
     * @param hbaseConn hbase 链接
     * @param nameSpace 命名空间
     * @param table     表名
     * @param rowKey    rowKey
     * @return 把一行查询到的所有列封装到一个 JSONObject 对象中
     */
    public static <T> T getRow(Connection hbaseConn,
                               String nameSpace,
                               String table,
                               String rowKey,
                               Class<T> tClass,
                               boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        try (Table Table = hbaseConn.getTable(TableName.valueOf(nameSpace, table))) { // jdk1.7 : 可以自动释放资源
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = Table.get(get);
            // 4. 把查询到的一行数据,封装到一个对象中: JSONObject
            // 4.1 一行中所有的列全部解析出来
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            T t = tClass.newInstance();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                /// 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
                if (defaultIsUToC) {
                    key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
                }
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                BeanUtils.setProperty(t, key, value);
            }
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //在HBaseUtil中补充异步查询相关方法
    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 异步的从 hbase 读取维度数据
     *
     * @param hBaseAsyncConn hbase 的异步连接
     * @param nameSpace      命名空间
     * @param tableName      表名
     * @param rowKey         rowKey
     * @return 读取到的维度数据, 封装到 json 对象中.
     */
    public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn,
                                          String nameSpace,
                                          String tableName,
                                          String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hBaseAsyncConn
                .getTable(TableName.valueOf(nameSpace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取 result
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            JSONObject dim = new JSONObject();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                dim.put(key, value);
            }

            return dim;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

}