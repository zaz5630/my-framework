package cn.azzhu.flink.stream.util;

import cn.azzhu.flink.stream.exception.TimeCharacteristicException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 工具类
 * @author azzhu
 * @since 2021-02-18 23:06
 */
public class FlinkUtils {

    /**
     * 根据用户传入的timeCharacteristic，设定env的处理时间
     * @param env
     * @param timecharacteristic
     */
    public static void handelTimeCharacteristic(StreamExecutionEnvironment env, String timecharacteristic) {
        if(timecharacteristic.equals(TimeCharacteristicEnum.EVENTTIME.getValue())) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }else if(timecharacteristic.equals(TimeCharacteristicEnum.INGESTIONTIME.getValue())) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        }else if(timecharacteristic.equals(TimeCharacteristicEnum.PROCESSINGTIME.getValue())) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        }else {
            throw new TimeCharacteristicException("请输入正确的处理时间格式");
        }

    }

    /**
     * 拼接用户配置文件中的kafkasourcetable的sql，加入connector等连接信息
     * @param kafkaTopic
     * @param kafkaBootstrapServers
     * @param kafkaGroupId
     * @param kafkaFormat
     * @param scanStartUpMode
     * @param createKafkaSourceSQL
     * @return
     */
    public static String getCreateKafkaSourceTableSQL(String kafkaTopic,String kafkaBootstrapServers,String kafkaGroupId,String kafkaFormat,String scanStartUpMode,String createKafkaSourceSQL) {

        String resultCreateTableSql = createKafkaSourceSQL + " WITH ( "
                +" 'connector' = 'kafka' ,"
                + " 'topic' = '" + kafkaTopic + "',"
                + " 'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "',"
                + " 'properties.group.id' = '" + kafkaGroupId + "',"
                + " 'format' = '" + kafkaFormat + "',"
                + " 'scan.startup.mode' = '" + scanStartUpMode + "',"
                + " 'json.fail-on-missing-field' = 'false',"
                + " 'json.ignore-parse-errors' = 'true' )";

        return resultCreateTableSql;
    }

    /**
     * 如果是debug模式，则每次运行前，删除上次运行时新建的表。如果不是debug模式，则不删除上次新建的表
     * @param tableEnv
     * @param isDebug
     * @param kafkaTableName
     * @param hiveTableName
     * @param tableType
     */
    public static void checkIsDebug(StreamTableEnvironment tableEnv, boolean isDebug, String kafkaTableName, String hiveTableName, Integer tableType) {
        if(isDebug) {
            if(tableType == TableTypeEnum.KAFKA.getType().intValue()) {
                tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
                tableEnv.executeSql("DROP TABLE IF EXISTS " +   kafkaTableName);
                tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
            }else if (tableType == TableTypeEnum.HIVE.getType().intValue()) {
                tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
                tableEnv.executeSql("DROP TABLE IF EXISTS " +   hiveTableName);
                tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
            }


        }
    }

    /**
     * 根据用户在配置文件中传入的createMysqlTableSQL拼接flinkSqlMysqlConnector
     * @param createMysqlTableSQL
     * @param mysqlUrl
     * @param mysqlTableName
     * @param mysqlUsername
     * @param mysqlPassword
     * @return
     */
    public static String getCreateMysqlTableSQL(String createMysqlTableSQL, String mysqlUrl,
                                                      String mysqlTableName, String mysqlUsername,
                                                      String mysqlPassword,String mysqlDriver) {
        return createMysqlTableSQL + " WITH ( " +
                " 'connector' = 'jdbc'," +
                " 'driver' = '" + mysqlDriver+  "'," +
                " 'url' = '" + mysqlUrl + "' ," +
                " 'table-name' = '" + mysqlTableName + "' ," +
                " 'username' = '" + mysqlUsername + "' , " +
                " 'password' = '" + mysqlPassword + "' )";

    }
    public static String getCreateDIMMysqlTableSQL(String createMysqlTableSQL, String mysqlUrl,
                                                      String mysqlTableName, String mysqlUsername,
                                                      String mysqlPassword,String mysqlDriver,String lookup_cache_max_rows,String lookup_cache_ttl) {
        return createMysqlTableSQL + " WITH ( " +
                " 'connector' = 'jdbc'," +
                " 'driver' = '" + mysqlDriver+  "'," +
                " 'url' = '" + mysqlUrl + "' ," +
                " 'table-name' = '" + mysqlTableName + "' ," +
                " 'username' = '" + mysqlUsername + "' , " +
                " 'password' = '" + mysqlPassword + "' ," +
                " 'lookup.cache.max-rows' = '" + lookup_cache_max_rows + "' ," +
                " 'lookup.cache.ttl' = '" + lookup_cache_ttl + "' )";

    }
}
