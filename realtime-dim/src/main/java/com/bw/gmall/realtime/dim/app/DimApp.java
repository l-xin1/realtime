package com.bw.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.base.BaseApp;
import com.bw.gmall.realtime.common.bean.TableProcessDim;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.until.FlinkSourceUtil;
import com.bw.gmall.realtime.common.until.HBaseUtil;
import com.bw.gmall.realtime.dim.function.HBaseSinkFunction;
import com.bw.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.common.network.Selectable;


import javax.annotation.security.DenyAll;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;


public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
       new DimApp().start(10001,1,"dim_app",Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStream) {
        // 对业务流数据处理 简单的ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStream);

       //TODO 使用FlinkCDC读取配置表信息

        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall2022_config", "table_process_dim");
        // 读取数据  封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");
        // mysql配置表数据打印
        mysqlStrDS.print();
        //
        // TODO 把配置流的数据转换  jsonStr->实体类
        SingleOutputStreamOperator<TableProcessDim> tpdDS = readTableProcess(mysqlStrDS);

        // 根据配置表的信息列 到hbase中 创建表  删除表操作
        tpdDS = createHBaseTable(tpdDS);
        // 使用广播流表 广播配置表信息
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDS, tpdDS);
        // 同步数据到hbase里
        dimDS.print();

        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tpdDS) {
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS
                = tpdDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS
                = jsonObjDS.connect(broadcastDS);

        //processElement 处理主流业务数据   根据维度表名到广播流读取配置表信息   判断是否为维度
        //processBroadcastElement  处理广播流配置信息  将配置数据中到广播流状态中或者删除状态对应的配置信息  维度表名  一个配置对象
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS
                = connectDS.process(new TableProcessFunction(mapStateDescriptor));
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpdDS) {
        tpdDS = tpdDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {


            private Connection HbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                HbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConn(HbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) throws Exception {
                //获取配置表进行的操作
                String op = tp.getOp();
                //获取Hbase里的维度表名字
                String sinkTable = tp.getSinkTable();
                //获取hbase里建表的列族
                String[] sinkFamily = tp.getSinkFamily().split(",");
                if ("d".equals(op)) {
                    HBaseUtil.dropHBaseTable(HbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                } else if ("r".equals(op) || "c".equals(op)) {
                    HBaseUtil.createHBaseTable(HbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamily);
                } else {
                    // 对配置表数据修改表   hbase里对应的表进行删除在创建表
                    HBaseUtil.dropHBaseTable(HbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                    HBaseUtil.createHBaseTable(HbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamily);
                }
                return tp;
            }
        });
        tpdDS.print();
        return tpdDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(DataStreamSource<String> mysqlStrDS) {
        SingleOutputStreamOperator<TableProcessDim> tpdDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        tpdDS.print();
        return tpdDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStream) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStream.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context,
                                               Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");
                        if (("gmall2022".equals(db)) && ("insert".equals(type) ||
                                "update".equals(type) ||
                                "delete".equals(type) ||
                                "bootstrap-insert".equals(type)) &&
                                data != null && data.length() > 2) {
                            collector.collect(jsonObj);
                        }
                    }
                }
        );
//        // kafka读取的数据过滤etl 打印控制台

//        jsonObjDS.print();
        return jsonObjDS;
    }
}