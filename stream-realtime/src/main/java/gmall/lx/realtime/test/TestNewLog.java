package gmall.lx.realtime.test;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import gmall.lx.realtime.common.KafkaNewUtil;
import gmall.lx.realtime.common.ProcessSplitStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hive.metastore.utils.StringUtils;

import java.util.HashMap;

/**
 * @className: TestNewLog
 * @author: lx
 * @date: 2024/12/25 10:27
 */
public class TestNewLog {
    private static final String kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_topic_group = ConfigUtils.getString("kafka.topic.group");
    private static final String kafka_err_log = ConfigUtils.getString("kafka.err.log");
    private static final String kafka_start_log = ConfigUtils.getString("kafka.start.log");
    private static final String kafka_display_log = ConfigUtils.getString("kafka.display.log");
    private static final String kafka_action_log = ConfigUtils.getString("kafka.action.log");
    private static final String kafka_dirty_topic = ConfigUtils.getString("kafka.dirty.topic");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");
    private static final OutputTag<String> errTag = new OutputTag<String>("errTag") {
    };
    private static final OutputTag<String> startTag = new OutputTag<String>("startTag") {
    };
    private static final OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
    };
    private static final OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
    };
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
    };
    private static final HashMap<String, DataStream<String>> collectDsMap = new HashMap<>();


    public static void main(String[] args) throws Exception {
//        CommonUtils.printCheckPropEnv(
//                false,
//                kafka_topic_base_log_data,
//                kafka_bootstrap_servers,
//                kafka_topic_group,
//                kafka_page_topic,
//                kafka_err_log,
//                kafka_start_log,
//                kafka_display_log,
//                kafka_action_log,
//                kafka_dirty_topic
//        );
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> kafkaSourceDS = env.fromSource(KafkaNewUtil.getKafka(kafka_bootstrap_servers, kafka_topic_base_log_data, kafka_topic_group),
                WatermarkStrategy.noWatermarks(), kafka_topic_group);

        SingleOutputStreamOperator<JSONObject> processDS = kafkaSourceDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    collector.collect(JSONObject.parseObject(s));
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                    System.err.println("Convert JsonData Error !");
                    throw new Exception();
                }
            }
        }).uid("convert_json_process").name("convert_json_process");

        SideOutputDataStream<String> dirtyDS = processDS.getSideOutput(dirtyTag);
        dirtyDS.print("dirtyDS->?");
        dirtyDS.sinkTo(KafkaNewUtil.toSinkKafka(kafka_bootstrap_servers, kafka_dirty_topic))
                .uid("sink_dirty_data_to_kafka").name("sink_dirty_data_to_kafka");

        KeyedStream<JSONObject, String> keyedStream = processDS.keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> mapDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastState;

            @Override
            public void open(Configuration parameters)  {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                lastState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                String lastDate = lastState.value();
                String ts = jsonObject.getString("ts");
                String curVisitDate = DateTimeUtils.tsToDate(Long.parseLong(ts));
                if ("1".equals(isNew)) {
                    if (StringUtils.isEmpty(lastDate)) {
                        //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        lastState.update(curVisitDate);
                    } else {
                        if (!lastDate.equals(curVisitDate)) {
                            //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    //如果 is_new 的值为 0
                    //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
                    // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                    if (StringUtils.isEmpty(lastDate)) {
                        String newDay = DateTimeUtils.tsToDate(Long.parseLong(ts) - 24 * 60 * 600 * 1000);
                        lastState.update(newDay);
                    }
                }
                return jsonObject;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        }).uid("fix_isNew_map").uid("fix_isNew_map");

        SingleOutputStreamOperator<String> processTagDS = mapDS.process(new ProcessSplitStream(errTag, startTag, displayTag, actionTag))
                .uid("process_json_stream").name("process_json_stream").setParallelism(1);
        SideOutputDataStream<String> sideOutputErrDS = processTagDS.getSideOutput(errTag);
        SideOutputDataStream<String> sideOutputStartDS = processTagDS.getSideOutput(startTag);
        SideOutputDataStream<String> sideOutputDisplayDS = processTagDS.getSideOutput(displayTag);
        SideOutputDataStream<String> sideOutputActionDS = processTagDS.getSideOutput(actionTag);

        collectDsMap.put("errTag", sideOutputErrDS);
        collectDsMap.put("startTag", sideOutputStartDS);
        collectDsMap.put("displayTag", sideOutputDisplayDS);
        collectDsMap.put("actionTag", sideOutputActionDS);
        collectDsMap.put("page", processTagDS);
        SplitToKafkaTag(collectDsMap);
        env.disableOperatorChaining();
        env.execute();
    }

    public static void SplitToKafkaTag(HashMap<String, DataStream<String>> dataStreamHashMap) {
        dataStreamHashMap.get("errTag").sinkTo(KafkaNewUtil.toSinkKafka(kafka_bootstrap_servers, kafka_err_log))
                .uid("sink_err").name("sink_err");
        dataStreamHashMap.get("startTag").sinkTo(KafkaNewUtil.toSinkKafka(kafka_bootstrap_servers, kafka_start_log))
                .uid("sink_startTag").name("sink_startTag");
        dataStreamHashMap.get("displayTag").sinkTo(KafkaNewUtil.toSinkKafka(kafka_bootstrap_servers, kafka_display_log))
                .uid("sink_displayTag").name("sink_displayTag");
        dataStreamHashMap.get("actionTag").sinkTo(KafkaNewUtil.toSinkKafka(kafka_bootstrap_servers, kafka_action_log))
                .uid("sink_actionTag").name("sink_actionTag");
        dataStreamHashMap.get("page").sinkTo(KafkaNewUtil.toSinkKafka(kafka_bootstrap_servers, kafka_page_topic))
                .uid("sink_page").name("sink_page");

    }

}
