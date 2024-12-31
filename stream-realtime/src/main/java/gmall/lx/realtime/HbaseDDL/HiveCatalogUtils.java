package gmall.lx.realtime.HbaseDDL;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @className: HiveCatalogUtils
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/30 14:02
 */
public class HiveCatalogUtils {
    private static final String HIVE_CONF_DIR = ConfigUtils.getString("hive.conf.dir");

    public static HiveCatalog getHiveCatalog(String catalogName){
        System.setProperty("HADOOP_USER_NAME","root");
        return new HiveCatalog(catalogName, "default", HIVE_CONF_DIR);
    }
}
