package gmall.lx.realtime.common;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.crypto.SecureUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.stream.common.domain.HBaseInfo;
import com.stream.common.utils.HbaseUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hbase.CellUtil.cloneQualifier;
import static org.apache.hadoop.hbase.CellUtil.cloneValue;


/**
 * @className: HbaseNewUtil
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/22 21:48
 */
public class HbaseNewUtil {
    private Connection connection;

    @SneakyThrows
    public static void main(String[] args) {
        HbaseUtils hbaseUtils = new HbaseUtils("cdh01");
//        hbaseUtils.getConnection();
    }

     public Connection getConnection(){
        return connection;
    }


    public HbaseNewUtil(String conn) throws Exception {
        Configuration entries = HBaseConfiguration.create();
        entries.set(HConstants.ZOOKEEPER_QUORUM, conn);
        // setting hbase "hbase.rpc.timeout" and "hbase.client.scanner.timeout" Avoidance scan timeout
        entries.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "1800000");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "1800000");
        // setting hbase "hbase.hregion.memstore.flush.size" buffer flush
        entries.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, "128M");
        entries.set("hbase.incremental.wal", "true");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "3600000");
        //entries.set(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,"1200000");
        this.connection = ConnectionFactory.createConnection(entries);
    }

    public boolean tableExists(String tables) throws Exception {
        Thread.sleep(1000);
        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(tables));
        admin.close();
        System.err.println("表:"+tables+(b? "存在":"不存在"));
        return b;
    }






}
