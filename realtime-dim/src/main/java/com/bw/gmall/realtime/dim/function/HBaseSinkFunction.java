package com.bw.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.bean.TableProcessDim;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.until.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * 将数据同步到HBase里
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn= HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(hbaseConn);
    }

    // 将流中的数据写入到hbase里
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
        JSONObject jsonObj = tup.f0;
        TableProcessDim tableProcessDim = tup.f1;
        String type = jsonObj.getString("type");
        jsonObj.remove("type");
        // 获取要操作的表名
        String sinkTable = tableProcessDim.getSinkTable();
        // 获取rowKey的值
        String sinkRowKey = tableProcessDim.getSinkRowKey();


        //判断业务数据进行了什么操作
        if ("delete".equals(type)){
            // 从业务数据维度中做数据删除操作
            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,sinkRowKey);
        }else {
            // 不是delete 其他所以类型向hbase里put添加数据
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkRowKey,sinkFamily,jsonObj);
        }
    }


}
