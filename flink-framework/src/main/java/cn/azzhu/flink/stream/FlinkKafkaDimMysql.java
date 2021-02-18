package cn.azzhu.flink.stream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * kafka关联维表
 * @author azzhu
 * @since 2021-02-18 23:06
 */
public class FlinkKafkaDimMysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);


        String createKafkaSourceTable = "CREATE TABLE kafkaSourceTable (\n" +
                " `num` INT,\n" +
                " `ts` BIGINT,\n" +
                " `vin` STRING,\n" +
                " `statusKeyValueMap` ROW(f1 STRING, f2 INT,f3 DOUBLE),\n" +
                " `rowtime` as TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000,'yyyy-MM-dd HH:mm:ss')), \n" +
                " `pts` AS PROCTIME() ,  \n" +
                " WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'flink_test',\n" +
                " 'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                " 'properties.group.id' = 'xx',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset' \n" +
                ")";

        String createMyslDimTable = "CREATE TABLE mysqlDimTable (\n" +
                "  id BIGINT,\n" +
                "  vin STRING,\n" +
                "  brand_name STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ") WITH ( \n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://node03:3306/canal_test',\n" +
                "   'table-name' = 'vin_brand' ,\n" +
                "   'username' = 'root' ,\n" +
//                "   'lookup.cache.max-rows' = '50000', \n" +
//                "   'lookup.cache.ttl' = '1min', \n" +
                "   'password' = 'root' \n" +
                ")";

        String createResultMysqlTable = "CREATE TABLE mysqlResult (\n" +
                "  vin STRING,\n" +
                "  brand_name STRING,\n" +
                "  f1 STRING,\n" +
                "  f2 INT,\n" +
                "  PRIMARY KEY (vin) NOT ENFORCED \n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc', \n" +
                "   'url' = 'jdbc:mysql://node03:3306/canal_test', \n" +
                "   'username' = 'root', \n" +
                "   'password' = 'root', \n" +
                "   'table-name' = 'mysql_result' \n" +
                ")";

        String insertMysql = "INSERT INTO mysqlResult select k.vin,m.brand_name,k.statusKeyValueMap.f1 AS f1,k.statusKeyValueMap.f2 AS f2 " +
                " from kafkaSourceTable k left JOIN mysqlDimTable  FOR SYSTEM_TIME AS OF k.pts AS m " +
                "  on k.vin = m.vin ";

        bsTableEnv.executeSql(createKafkaSourceTable);
        bsTableEnv.executeSql(createMyslDimTable);
        bsTableEnv.executeSql(createResultMysqlTable);
        bsTableEnv.executeSql(insertMysql);
    }

}
