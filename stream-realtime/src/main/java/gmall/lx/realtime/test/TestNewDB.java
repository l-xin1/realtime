package gmall.lx.realtime.test;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import gmall.lx.realtime.common.KafkaNewUtil;
import gmall.lx.realtime.common.MysqlCDCSource;
import gmall.lx.realtime.common.ProcessToKafkaDwd;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @className: TestDB
 * @author: lx
 * @date: 2024/12/25 18:37
 */
public class TestNewDB {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> mysqlSource = MysqlCDCSource.getMysqlSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial());
        DataStreamSource<String> dataStreamSource = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "sourceTables");
        MySqlSource<String> mysqlSourceDwdDS = MysqlCDCSource.getMysqlSource(
                ConfigUtils.getString("mysql.database.dwd"),
                ConfigUtils.getString("mysql.table.dwd"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        DataStreamSource<String> dataStreamSourceDwdDS = env.fromSource(mysqlSourceDwdDS, WatermarkStrategy.noWatermarks(), "process_dwd");
        SingleOutputStreamOperator<JSONObject> dataStreamSourceMapDS = dataStreamSource.map(JSONObject::parseObject)
                .uid("map_json_data_source").name("map_json_data_source");
        SingleOutputStreamOperator<JSONObject> dataStreamSourceDwdMapDS = dataStreamSourceDwdDS.map(JSONObject::parseObject)
                .uid("map_json_data_source_dwd").name("map_json_data_source_dwd");
//        SingleOutputStreamOperator<JSONObject> tpDS = dataStreamSourceDwdMapDS.map(m -> {
//            m.remove("source");
//            JSONObject jsonObj = new JSONObject();
//            if ("d".equals(m.getString("op"))) {
//                jsonObj.put("before", m.getJSONObject("before"));
//            } else {
//                jsonObj.put("after", m.getJSONObject("after"));
//            }
//            jsonObj.put("op", m.getString("op"));
//            return jsonObj;
//        }).uid("map_json_data_source_tpDS").name("map_json_data_source_tpDS").setParallelism(1);
//
//        MapStateDescriptor<String, JSONObject> mapState = new MapStateDescriptor<>("mapState", String.class, JSONObject.class);
//        BroadcastStream<JSONObject> broadcastDS = tpDS.broadcast(mapState);
//        BroadcastConnectedStream<JSONObject, JSONObject> connectDS = dataStreamSourceMapDS.connect(broadcastDS);
//        SingleOutputStreamOperator<String> streamOperator = connectDS.process(new ProcessToKafkaDwd(mapState));
        OutputTag<String> couponUse = new OutputTag<String>("coupon_use") {};
        OutputTag<String> favorInfo = new OutputTag<String>("favor_info") {};
        OutputTag<String> userInfo = new OutputTag<String>("user_info") {};
        SingleOutputStreamOperator<String> outputStreamOperator = dataStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                if (jsonObject.getJSONObject("source").getString("table").equals("coupon_use")) {
                    context.output(couponUse,jsonObject.getJSONObject("after").toJSONString());
                }

                if (jsonObject.getJSONObject("source").getString("table").equals("favor_info")) {
                    context.output(favorInfo,jsonObject.getJSONObject("after").toJSONString());
                }

                if (jsonObject.getJSONObject("source").getString("table").equals("user_info")) {
                    context.output(userInfo,jsonObject.getJSONObject("after").toJSONString());
                }

            }
        });
        outputStreamOperator.getSideOutput(couponUse).sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092", "dwd_coupon_use"));
        outputStreamOperator.getSideOutput(userInfo).sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092", "dwd_user_info"));
        outputStreamOperator.getSideOutput(favorInfo).sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092", "dwd_favor_info"));
        env.disableOperatorChaining();
        env.execute();
    }
}
