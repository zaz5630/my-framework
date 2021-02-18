package cn.azzhu.flink.stream;

import cn.azzhu.flink.stream.util.FlinkUtils;
import cn.azzhu.flink.stream.util.PropertiesReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HiveCatlog
 * @author azzhu
 * @since 2021-02-18 23:06
 */
public class KafkaToHiveCatlog {
    final static Logger logger = LoggerFactory.getLogger(KafkaToHiveCatlog.class);

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
        String alterHiveTableSQL = propertiesReader.getProperties("alterHiveTableSQL");
        String insertSQL = propertiesReader.getProperties("insertSQL");


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 判断项目的处理时间，EventTime or ProcessTime or IngestionTime
        FlinkUtils.handelTimeCharacteristic(env,timecharacteristic);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(90 * 1000);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
//        RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration = new RestartStrategies.FailureRateRestartStrategyConfiguration(3, Time.minutes(5), Time.seconds(10));
//        env.setRestartStrategy(restartStrategyConfiguration);

        final EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //注册hive catalog
        String catalogName = "my_catalog";
        String defaultDatabase = "default";
        HiveCatalog catalog = new HiveCatalog(
                catalogName,              // catalog name
                defaultDatabase,                // default database
                "/etc/hive/conf",  // Hive config (hive-site.xml) directory
                "3.1.0"                   // Hive version
        );
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        String kafkaDBname = kafkaTableName.split("\\.")[0];
        String kafkaTbname = kafkaTableName.split("\\.")[1];
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS "+kafkaDBname);

        //1.判断catlog中hivesinktable是否存在
        String hiveDBname = hiveTableName.split("\\.")[0];
        String hiveTbname = hiveTableName.split("\\.")[1];

        ObjectPath hiveTbop = new ObjectPath(hiveDBname,hiveTbname);
        if(isDebug){
            System.out.println("删除hive表");
            tableEnv.getCatalog(catalogName).get().dropTable(hiveTbop,true);
        }
        logger.info(hiveTbname+"是否存在：  "+tableEnv.getCatalog(catalogName).get().tableExists(hiveTbop)+"");
        boolean isHiveSinkTable =tableEnv.getCatalog(catalogName).get().tableExists(hiveTbop);


        System.out.println("hive 表是否存在："+isHiveSinkTable);
        //2.删除catlog中kafkatable，每次重建，如果字段修改直接修改配置文件create语句，不需要alter语句
        //if set to false, throw an exception,
	    //if set to true, do nothing
        ObjectPath kafkaTableop = new ObjectPath(kafkaDBname,kafkaTbname);
        tableEnv.getCatalog(catalogName).get().dropTable(kafkaTableop,true);
        //3.重建catlog中kafkatable
        final String createKafkaSourceTableSQL = FlinkUtils.getCreateKafkaSourceTableSQL(kafkaTopic,kafkaBootstrapServers,kafkaGroupId,kafkaFormat,scanStartUpMode,createKafkaSourceSQL);
        tableEnv.executeSql(createKafkaSourceTableSQL);
        //4.根据第一步结果，如果不存在则执行创建语句
        System.out.println("创建 kafkatable "+kafkaTableName+" 完毕");

        if(!isHiveSinkTable){
            System.out.println("创建hive表");
            tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
            tableEnv.executeSql(createHiveSinkSQL);
        }

//        List<TableColumn> cols = tableEnv.getCatalog(catalogName).get().getTable(hiveTbop).getSchema().getTableColumns();
//        for (int j = 0; j <cols.size() ; j++) {
//            System.out.println(hiveTableName+" col--->"+cols.get(j).getName());
//        }
//
//        List<TableColumn> kafkacols = tableEnv.getCatalog(catalogName).get().getTable(kafkaTableop).getSchema().getTableColumns();
//        for (int j = 0; j <kafkacols.size() ; j++) {
//            System.out.println(kafkaTableName+" col--->"+kafkacols.get(j).getName());
//        }

        //5.判断配置文件是否包含hive alter语句，如果有则执行。alter语句一定在建表后执行

        if(alterHiveTableSQL!=null&& StringUtils.isNotBlank(alterHiveTableSQL)){
            System.out.println("执行alert");
            tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
            tableEnv.executeSql(alterHiveTableSQL);
        }


        //6.执行查询sql并写入hive，查询kafkasourcetable的内容，得到查询结果，并准备插入到hive表中
        System.out.println("insert");
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(insertSQL);

    }
}


