package gmall.com.deploy.toyarn;

import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.*;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.util.Collections;

/**
 * @className: FlinkJobSubmitToYarnModel
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/26 20:51
 */
public class FlinkJobSubmitToYarnModel {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJobSubmitToYarnModel.class);
    private static final String FLINK_SUBMIT_USER = ConfigUtils.getString("flink.submit.user");
    private static final String FLINK_COMMON_CONF_DIR = ConfigUtils.getString("flink.conf.configurationDirectory");
    private static final String FLINK_CLUSTER_LIBS_DIR = ConfigUtils.getString("flink.cluster.libs");

    public static void main(String[] args) {
        CommonUtils.printCheckPropEnv(false,FLINK_SUBMIT_USER,FLINK_COMMON_CONF_DIR,FLINK_CLUSTER_LIBS_DIR);



    }

    public static void SubFlinkTask(String submitUser,
                                    String configurationDirectory,
                                    String flinkClusterLibDir,
                                    String userJarPath,
                                    String jobName,
                                    String fullClassName){
        System.setProperty("HADOOP_USER_NAME",submitUser);
        String flinkDistJar="hdfs://cdh01:8020/flink-dist/lin/";
        YarnClient yarnClient = YarnClient.createYarnClient();
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        YarnClientYarnClusterInformationRetriever clusterInformationRetriever = YarnClientYarnClusterInformationRetriever
                .create(yarnClient);
        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS,true);
        flinkConfiguration.set(
                PipelineOptions.JARS,
                Collections.singletonList(userJarPath)
        );

        Path remoteLib = new Path(flinkClusterLibDir);
        flinkConfiguration.set(YarnConfigOptions.PROVIDED_LIB_DIRS,
                Collections.singletonList(remoteLib.toString()));

        flinkConfiguration.set(
                YarnConfigOptions.FLINK_DIST_JAR,
                flinkDistJar
        );

        // 设置application 模式
        flinkConfiguration.set(DeploymentOptions.TARGET,
                YarnDeploymentTarget.APPLICATION.getName());

        flinkConfiguration.set(YarnConfigOptions.APPLICATION_QUEUE,
                "default");
        flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME,jobName);
        YarnLogConfigUtil.setLogConfigFileInConfig(flinkConfiguration,configurationDirectory);

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();

        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{"test"}, fullClassName);

        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                flinkConfiguration,
                yarnConfiguration,
                yarnClient,
                clusterInformationRetriever,
                true
        );

        try {
            ClusterClientProvider<ApplicationId> clusterClientProvider = yarnClusterDescriptor.deployApplicationCluster(
                    clusterSpecification, appConfig
            );
            ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();

            ApplicationId applicationId = clusterClient.getClusterId();
            String webInterfaceURL = clusterClient.getWebInterfaceURL();
            LOG.info("applicationID is {}",applicationId);
            LOG.info("webInterfaceUEL is {}",webInterfaceURL);
        } catch (ClusterDeploymentException e) {
            LOG.error(e.getMessage(),e);
        }

    }


}
