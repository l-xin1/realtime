package gmall.lx.realtime.test;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.JdbcUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

/**
 * @className: TextJdbc
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/27 11:29
 */
public class TextJdbc {


    public static void main(String[] args) throws Exception {
//        HashMap<String, JSONObject> configMap = new HashMap<>();
//        Connection mySQLConnection = JdbcUtils.getMySQLConnection("jdbc:mysql://cdh03:3306?useSSL=false", "root", "root");
//        List<JSONObject> jsonObjects = JdbcUtils.queryList(mySQLConnection,
//                "select * from gmall_process.table_process_dwd",
//                JSONObject.class, true);
//        for (JSONObject jsonObject : jsonObjects) {
//            String sourceTable = jsonObject.getString("sourceTable");
//            configMap.put(sourceTable,jsonObject);
//        }
//        System.out.println(configMap);
//        System.err.println(configMap.get("user_info").toString());
//        mySQLConnection.close();

        for (int i = 1; i < 11; i++) {
            System.out.println("这个男人是第"+i+"位");
        }


    }
}
