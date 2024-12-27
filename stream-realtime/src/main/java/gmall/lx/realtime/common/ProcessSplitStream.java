package gmall.lx.realtime.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @className: ProcessSplitStream
 * @author: lx
 * @date: 2024/12/25 11:54
 */
public class ProcessSplitStream extends ProcessFunction<JSONObject, String> {
    private OutputTag<String> errTag;
    private OutputTag<String> startTag;
    private OutputTag<String> displayTag;
    private OutputTag<String> actionTag;

    public ProcessSplitStream(OutputTag<String> errTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        this.errTag = errTag;
        this.startTag = startTag;
        this.displayTag = displayTag;
        this.actionTag = actionTag;
    }

    @Override
    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
        JSONObject errJson = jsonObject.getJSONObject("err");
        if (errJson != null) {
            context.output(errTag, errJson.toJSONString());
            jsonObject.remove("err");
        }

        JSONObject startJsonObj = jsonObject.getJSONObject("start");
        if (startJsonObj != null) {
            context.output(startTag, jsonObject.toJSONString());
        } else {
            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
            String ts = jsonObject.getString("ts");
            JSONArray displaysArr = jsonObject.getJSONArray("displays");
            if (displaysArr != null && displaysArr.size() > 0) {
                for (int i = 0; i < displaysArr.size(); i++) {
                    JSONObject displayJsonObj = displaysArr.getJSONObject(i);
                    JSONObject newJsonObj = new JSONObject();
                    newJsonObj.put("common", commonJsonObj);
                    newJsonObj.put("page", pageJsonObj);
                    newJsonObj.put("display", displayJsonObj);
                    newJsonObj.put("ts", ts);
                    context.output(displayTag, newJsonObj.toJSONString());
                }
                jsonObject.remove("displays");
            }
            JSONArray actionArr = jsonObject.getJSONArray("actions");
            if (actionArr != null && actionArr.size() > 0) {
                for (int i = 0; i < actionArr.size(); i++) {
                    JSONObject actionArrJSONObject = actionArr.getJSONObject(i);
                    JSONObject actJsonObj = new JSONObject();
                    actJsonObj.put("common", commonJsonObj);
                    actJsonObj.put("page", pageJsonObj);
                    actJsonObj.put("action", actionArrJSONObject.toJSONString());
                    context.output(actionTag, actJsonObj.toJSONString());
                }
                jsonObject.remove("actions");
            }
            collector.collect(jsonObject.toJSONString());

        }


    }
}
