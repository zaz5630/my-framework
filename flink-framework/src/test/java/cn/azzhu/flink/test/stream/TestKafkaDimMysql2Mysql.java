package cn.azzhu.flink.test.stream;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Kafka 关联 mysql 维度表
 * @author azzhu
 * @since 2021-02-18 23:29
 */
public class TestKafkaDimMysql2Mysql {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String kafkaSourceTableTbox = "CREATE TABLE kafkaSourceTableTbox (\n" +
                " `timestamp` BIGINT,\n" +
                " `statusKeyValueMap` ROW(`5`  int,`6` int) ,\n" +
                " `platformTime` BIGINT ,\n" +
                " `terminalTime` BIGINT ,\n" +
                " `vin` STRING ,\n" +
                " `version` INT ,\n" +
                " `businessSerialNum` INT ,\n" +
                " `protocolSerialNum` INT , \n" +
                " `pts` AS PROCTIME()  \n" +  //处理时间
                "  t as TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000,'yyyy-MM-dd HH:mm:ss')) , \n" +  //事件时间
                "  WATERMARK FOR t AS t - INTERVAL '2' SECOND \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'topic',\n" +
                " 'properties.bootstrap.servers' = 'ip:9092',\n" +
                " 'properties.group.id' = 'kkb',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset' \n" +
                ")";


        String mysqlSourceTable = "CREATE TABLE mysqlSourceTable (\n" +
                " `id` BIGINT,\n" +
                " `vin` STRING,\n" +
                " `brand_id` BIGINT,\n" +
                " `brand_name` STRING,\n" +
                " `veh_series_id` BIGINT,\n" +
                " `veh_series_name` STRING,\n" +
                " `veh_model_id` BIGINT,\n" +
                " `veh_model_name` STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'url' = 'jdbc:mysql://ip:3306/db_device?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE&failOverReadOnly=false',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root',\n" +
                "   'lookup.cache.max-rows' = '5000',\n" +
                "   'lookup.cache.ttl' = '10min',\n" +
                "   'table-name' = 'tb_vehicle'\n" +
                ")";


        String mysqlSinkTable = "CREATE TABLE mysqlSinkTable (\n" +
                "  `vin` STRING,\n" +
                "  `brand_id` BIGINT, \n" +
                "  `veh_series_id` BIGINT, \n" +
                "  `veh_model_id` BIGINT, \n" +
                "  `longitude` int, \n" +
                "  `latitude` int, \n" +
                "  `msgtime` BIGINT, \n" +
                "  PRIMARY KEY (vin) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/mytest?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE&failOverReadOnly=false',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root',\n" +
                "   'table-name' = 'result' \n" +
                ")";

//        tableEnv.executeSql(kafkaSourceTableTbox);
        tableEnv.executeSql(mysqlSourceTable);
//        tableEnv.executeSql(mysqlSinkTable);

        Table table = tableEnv.sqlQuery("select * from mysqlSourceTable");
        tableEnv.toAppendStream(table, Row.class).print();
        env.execute("test");


//        String windowQuerySql = "select a.* from kafkaSourceTableTbox a " +
//                "inner join (select vin,max(`timestamp`) as ts " +
//                "from kafkaSourceTableTbox GROUP BY vin,TUMBLE(pts, INTERVAL '5' SECOND)) as b on a.vin = b.vin and a.`timestamp` = b.ts";
//        final Table table = tableEnv.sqlQuery(windowQuerySql);
//        tableEnv.createTemporaryView("windowTable", table);
//
//        //join msyql table 并落地mysql
//
//        String querySql = "SELECT k.vin AS vin,m.brand_id AS brand_id,m.veh_series_id AS veh_series_id, m.veh_model_id AS veh_model_id, " +
//                " k.statusKeyValueMap.`5` AS longitude,k.statusKeyValueMap.`6` AS latitude, k.`timestamp` AS msgtime  " +
//                " from windowTable AS k left join mysqlSourceTable  FOR SYSTEM_TIME AS OF k.pts AS m ON k.vin = m.vin";
//
//        final Table joinTable = tableEnv.sqlQuery(querySql);
//
//        tableEnv.createTemporaryView("join_view", joinTable);
//        tableEnv.executeSql("insert into mysqlSinkTable select * from join_view");
    }
}
