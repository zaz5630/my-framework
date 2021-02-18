package cn.azzhu.flink.stream;

import cn.azzhu.flink.stream.util.FlinkUtils;
import cn.azzhu.flink.stream.util.PropertiesReader;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * Kafka写入到hive
 * @author azzhu
 * @since 2021-02-18 23:06
 */
public class KafkaToHive {
    public static void main(String[] args) throws Exception {
        if(args.length <1 ) {
            System.err.println("flink框架需要配置文件，请输入配置文件路径");
            return;
        }
        String filePath = args[0];
        //初始化配置文件变量 init
        PropertiesReader propertiesReader = new PropertiesReader(filePath);
        String timecharacteristic = propertiesReader.getProperties("timecharacteristic");
        boolean isDebug = Boolean.parseBoolean(propertiesReader.getProperties("isDebug"));
        String kafkaTableName = propertiesReader.getProperties("kafkaTableName");
        String createKafkaSourceSQL = propertiesReader.getProperties("createKafkaSourceSQL");
        String kafkaTopic = propertiesReader.getProperties("kafka.topic");
        String kafkaBootstrapServers = propertiesReader.getProperties("kafka.bootstrap.servers");
        String kafkaGroupId = propertiesReader.getProperties("kafka.group.id");
        String kafkaFormat = propertiesReader.getProperties("kafka.format");
        String scanStartUpMode = propertiesReader.getProperties("scan.startup.mode");
        String hiveTableName = propertiesReader.getProperties("hiveTableName");
        String createHiveSinkSQL = propertiesReader.getProperties("createHiveSinkSQL");
        String insertSQL = propertiesReader.getProperties("insertSQL");


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 判断项目的处理时间，EventTime or ProcessTime or IngestionTime
        FlinkUtils.handelTimeCharacteristic(env,timecharacteristic);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(90 * 1000);

        final EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);



        //注册hive catalog
        String catalogName = "my_catalog";
        HiveCatalog catalog = new HiveCatalog(
                catalogName,              // catalog name
                "default",                // default database
                "/etc/hive/conf",  // Hive config (hive-site.xml) directory
                "3.1.0"                   // Hive version
        );
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);


        if(isDebug) {
            tableEnv.executeSql("drop table IF EXISTS " + kafkaTableName);
            tableEnv.executeSql("drop table IF EXISTS " + hiveTableName);
        }

        //创建kafka的source表，表名由用户从配置文件中传入

        final String createKafkaSourceTableSQL = FlinkUtils.getCreateKafkaSourceTableSQL(kafkaTopic,kafkaBootstrapServers,kafkaGroupId,kafkaFormat,scanStartUpMode,createKafkaSourceSQL);
        tableEnv.executeSql(createKafkaSourceTableSQL);

        //创建hive的表，表名由用户从配置文件中传入
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql(createHiveSinkSQL);

        //查询kafkasourcetable的内容，得到查询结果，并准备插入到hive表中
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        //将querytable插入到hive中
        tableEnv.executeSql(insertSQL);

    }
}
