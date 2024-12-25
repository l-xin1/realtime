package gmall.lx.realtime.test;

import com.alibaba.fastjson.JSONObject;
import gmall.lx.realtime.common.KafkaNewUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.text.SimpleDateFormat;

/**
 * @className: TestLog
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/24 10:23
 */
public class TestLog {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> source = KafkaNewUtil.getKafka("cdh01:9092", "topic_log", "group12");

        SingleOutputStreamOperator<String> sourceLog = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .uid("log_kafka_json_10").name("log_kafka_json_10");
//        sourceLog.print();
        OutputTag<String> outputTag = new OutputTag<String>("oldTag") {
        };
        SingleOutputStreamOperator<JSONObject> logJson = sourceLog
                .filter(new FilterFunction<String>() {
                            @Override
                            public boolean filter(String s) throws Exception {
                                try {
                                    if (s != null || s.length() > 0) {
                                        // JSONObject.parseObject(s);
                                        return true;
                                    }
                                } catch (Exception e) {
                                    System.err.println("错误数据:->" + e);
                                    return false;
                                }
                                return false;
                            }
                        }
                ).map(JSONObject::parseObject).uid("log_filter_json_11").name("log_filter_json_11").setParallelism(1);
//        logJson.print();
        SingleOutputStreamOperator<JSONObject> streamOperator = logJson.keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration p) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<String>("stateString", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject,
                                               KeyedProcessFunction<String, JSONObject, JSONObject>.Context context,
                                               Collector<JSONObject> collector) throws Exception {
                        JSONObject common = jsonObject.getJSONObject("common");
                        String isNew = common.getString("is_new");
                        Long ts = Long.parseLong(jsonObject.getString("ts"));
//                        System.out.println(ts);
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
                        String dateOld = dateFormat.format(ts);
//                        System.err.println(dateOld);
                        String firstValue = state.value();
                        if (isNew.equals("1")) {
                            if (firstValue == null) {
                                state.update(dateOld);
                            } else if (!dateOld.equals(firstValue)) {
                                common.put("is_new", "0");
                            }
                        } else {
                            if (firstValue == null) {
                                state.update(dateFormat.format(ts - (24 * 60 * 60 * 1000)));
                            }
                        }

                        collector.collect(jsonObject);
                    }
                }).uid("log_is_new_json_12").name("log_is_new_json_12").setParallelism(1);
//        streamOperator.print();
        OutputTag<JSONObject> page = new OutputTag<JSONObject>("page") {};
        OutputTag<JSONObject> display = new OutputTag<JSONObject>("display") {};
        OutputTag<JSONObject> action = new OutputTag<JSONObject>("action") {};
        OutputTag<JSONObject> err = new OutputTag<JSONObject>("err") {};
        SingleOutputStreamOperator<JSONObject> outputStreamOperator = streamOperator.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                if (jsonObject.getString("start") != null) {
                    collector.collect(jsonObject);
                }

                if (jsonObject.getString("page") != null) {
                    context.output(page, jsonObject);
                }
                if (jsonObject.getString("err") != null) {
                    context.output(err, jsonObject);
                }
                if (jsonObject.getString("actions") != null) {
                    context.output(action, jsonObject);
                }
                if (jsonObject.getString("displays") != null) {
                    context.output(display, jsonObject);
                }
            }
        });
//        outputStreamOperator.print();
//        outputStreamOperator.getSideOutput(page).print("page-?");
//        outputStreamOperator.getSideOutput(err).print("err-?");
//        outputStreamOperator.getSideOutput(action).print("action-?");
//        outputStreamOperator.getSideOutput(display).print("display-?");

//        outputStreamOperator.map(m->m.toString()).sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092","topic_start"));
        outputStreamOperator.getSideOutput(page).map(JSONObject::toString).sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092","topic_page"));
//        outputStreamOperator.getSideOutput(err).map(m->m.toString()).sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092","topic_err"));
//        outputStreamOperator.getSideOutput(action).map(m->m.toString()).sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092","topic_action"));
//        outputStreamOperator.getSideOutput(display).map(m->m.toString()).sinkTo(KafkaNewUtil.toSinkKafka("cdh01:9092","topic_display"));
        env.execute();
    }
}
