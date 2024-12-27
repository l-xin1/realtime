package gmall.lx.realtime.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @className: ProcessToKafkaDwd
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/25 20:45
 */
public class ProcessToKafkaDwd extends BroadcastProcessFunction<JSONObject, JSONObject, String> {
    private MapStateDescriptor<String, JSONObject> mapState;
    private final HashMap<String, JSONObject> configMap = new HashMap<>();

    public ProcessToKafkaDwd(MapStateDescriptor<String,        JSONObject> mapState) {
        this.mapState = mapState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mySQLConnection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("kafka.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));

        String sql = "select * from gmall_process.table_process_dwd";
        List<JSONObject> tableProcessDwdDS = JdbcUtils.queryList(mySQLConnection, sql, JSONObject.class, true);
        for (JSONObject tableProcessDwdD : tableProcessDwdDS) {
            configMap.put(tableProcessDwdD.getString("sourceTable"), tableProcessDwdD);
//            System.err.println(configMap);
        }

        JdbcUtils.closeMySQLConnection(mySQLConnection);

    }

    @Override
    public void processElement(JSONObject jsonObject,
                               BroadcastProcessFunction<JSONObject, JSONObject, String>.ReadOnlyContext readOnlyContext,
                               Collector<String> collector) throws Exception {
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapState);
        String tableName = jsonObject.getJSONObject("source").getString("table");

        JSONObject broadData = broadcastState.get(tableName);
        JSONObject newJsonObj = new JSONObject();
        if (broadData != null && configMap.get(tableName) != null) {
            if (configMap.get(tableName).getString("sourceTable").equals(tableName)) {
                JSONObject newJson = new JSONObject();
                String op = jsonObject.getString("op");
                newJson.put("op",op);
                JSONObject after = jsonObject.getJSONObject("after");
                newJson.put("after",after);
                newJson.put("table", tableName);
//                String sinkTable = configMap.get(tableName).getString("sinkTable");
//                String sourceType = configMap.get(tableName).getString("sourceType");
                newJsonObj.put("table", tableName);
//                newJsonObj.put("source_type", sourceType);
//                newJsonObj.put("op", jsonObject.getString("op"));
//                newJsonObj.put("ts", jsonObject.getString("ts_ms"));
//                JSONObject after = jsonObject.getJSONObject("after");
//                newJsonObj.put("sink_table", sinkTable);
//                newJsonObj.put("after", after);
//                ArrayList<String> SinkColumn = new ArrayList<>(Arrays.asList(configMap.get(tableName).getString("sinkColumns").split(",")));
//                newJsonObj.getJSONObject("after").keySet().removeIf(key -> !SinkColumn.contains(key));
//                collector.collect(newJsonObj.toJSONString());
                collector.collect(newJson.toJSONString());
            }
        }

    }
    /**
     * if (sourceType.equals("insert")){
     *                     newJsonObj.put("table", tableName);
     *                     newJsonObj.put("source_type", sourceType);
     *                     newJsonObj.put("op", jsonObject.getString("op"));
     *                     newJsonObj.put("ts", jsonObject.getString("ts_ms"));
     *                     JSONObject after = jsonObject.getJSONObject("after");
     *                     newJsonObj.put("sink_table", sinkTable);
     *                     newJsonObj.put("after", after);
     *                     ArrayList<String> SinkColumn = new ArrayList<>(Arrays.asList(configMap.get(tableName).getJSONObject("after").getString("sink_columns").split(",")));
     *                     newJsonObj.getJSONObject("after").keySet().removeIf(key -> !SinkColumn.contains(key));
     *                     collector.collect(newJsonObj.toJSONString());
     *                 } else if (sourceType.equals("update"))
     */

    @Override
    public void processBroadcastElement(JSONObject jsonObject,
                                        BroadcastProcessFunction<JSONObject, JSONObject, String>.Context context,
                                        Collector<String> collector) throws Exception {
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapState);
        String op = jsonObject.getString("op");
        if (jsonObject.containsKey("after")) {
            String source_tableDS = jsonObject.getJSONObject("after").getString("source_table");
            if ("d".equals(op)) {
                broadcastState.remove(source_tableDS);
            } else {
                broadcastState.put(source_tableDS, jsonObject);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
