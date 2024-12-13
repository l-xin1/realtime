package com.bw.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.bean.TableProcessDim;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.until.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * 处理主流和广播流业务数据
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String,TableProcessDim> configMap=new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表的配置表信息加载到程序configMap
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall2022_config.table_process_dim ", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }

        JdbcUtil.closeMysqlConnection(mysqlConnection);

//        //注册驱动   建立连接   //获取数据库操作对象
//        Class.forName("com.mysql.cj.jdbc.Driver");
//        java.sql.Connection conn = DriverManager
//                .getConnection(Constant.MYSQL_URL,Constant.MYSQL_USER_NAME,Constant.MYSQL_PASSWORD);
//        // 获取数据库操作对象
//        String sql=" select * from gmall2022_config.table_process_dim ";
//        PreparedStatement ps = conn.prepareStatement(sql);
//        ResultSet resultSet = ps.executeQuery();
//        ResultSetMetaData metaData = resultSet.getMetaData();
//        //处理结果集
//        while (resultSet.next()){
//            JSONObject jsonObject = new JSONObject();
//            for (int i = 1; i < metaData.getColumnCount(); i++) {
//                String columnName = metaData.getColumnName(i);
//                String columnValue = resultSet.getString(i);
//                jsonObject.put(columnName,columnValue);
//            }
//            //将json 转换为实体类对象  并放到configMap中
//            TableProcessDim tableProcessDim = jsonObject.toJavaObject(TableProcessDim.class);
//            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
//        }
//
//        //释放资源
//        resultSet.close();
//        ps.close();
//        conn.close();


    }


    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        String table = jsonObject.getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState
                = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcessDim tableProcessDim = null;


//                String sinkRowKey = tableProcessDim.getSinkRowKey();

        if ((tableProcessDim=broadcastState.get(table)) != null
                || (tableProcessDim=configMap.get(table))!=null) {

            JSONObject data = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(data,sinkColumns);


            String type = jsonObject.getString("type");
            data.put("type",type);


            collector.collect(Tuple2.of(data, tableProcessDim));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        String op = tp.getOp();

        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String sourceTable = tp.getSourceTable();
        if ("d".equals(op)) {
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            broadcastState.put(sourceTable, tp);
            configMap.put(sourceTable,tp);
        }
    }


    // 过滤掉不需要传递的字段
    private static void deleteNotNeedColumns(JSONObject data, String sinkColumns) {
//        String[] columns = sinkColumns.split(",");
//        JSONObject newJsonObj = new JSONObject();
//        for (String column : columns) {
//            newJsonObj.put(column,data.getString(column));
//        }
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = data.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }


}
// iterator
//Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();





// old
//        for (Map.Entry<String, Object> entry : entrySet) {
//            if (!columnList.contains(entry.getKey())){
//                entrySet.remove(entry);
//            }
//        }

//        List aaList = new ArrayList();
//        aaList.add(1);
//        aaList.add(2);
//        aaList.add(3);
//        for (Object o : aaList) {
//            aaList.remove(o);
//        }



