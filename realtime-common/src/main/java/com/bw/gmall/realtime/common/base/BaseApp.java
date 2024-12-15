package com.bw.gmall.realtime.common.base;

import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.until.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 广 播  流
import java.security.PublicKey;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
public abstract class BaseApp {
    public  void start(int port,int parallelism,String ckAndGroupId,String topic) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //设置并行度
        env.setParallelism(parallelism);
        // 开始检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
////         设置job取消检查是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
////         设置两个检查点之间最小间距
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
////         设置重启
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
////        设置状态后端 和检查点储存路径
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/rk6/week2/"+ckAndGroupId);
//
////        设置操作hadoop的用户
//        System.setProperty("Hadoop_USER_NAME","hadoop");


        //TODO 从kafka读取topic_db数据

        KafkaSource<String> kafkaSource
                = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);
        // 消费数据   封装为流
        DataStreamSource<String> kafkaStream =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");
        // 处理逻辑
        handle(env,kafkaStream);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public  abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> stream);



}
