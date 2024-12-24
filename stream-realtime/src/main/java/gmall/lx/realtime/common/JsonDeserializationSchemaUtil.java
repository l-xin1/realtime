package gmall.lx.realtime.common;

import com.alibaba.fastjson.JSONObject;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


public class JsonDeserializationSchemaUtil implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sr, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();
        JSONObject data = new JSONObject();

        String[] topics = sr.topic().split("\\.");
        String db = topics[1];
        String tb = topics[2];
        Struct value = (Struct) sr.value();
        String op = value.getString("op");
        Struct after = value.getStruct("after");
        if(after!=null){
            List<Field> fields = after.schema().fields();
            for(Field f:fields){
                String n = f.name();
                Object v = after.get(f);
                data.put(n,v);
            }
        }
        result.put("db",db);
        result.put("tb",tb);
        result.put("op",op);
        result.put("data",data);

        collector.collect(result.toString());



    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
