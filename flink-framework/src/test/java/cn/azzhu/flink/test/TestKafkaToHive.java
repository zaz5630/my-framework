package cn.azzhu.flink.test;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author azzhu
 * @since 2021-02-18 23:29
 */
public class TestKafkaToHive {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        String kafkaSourceTable = "CREATE TABLE kafkaSourceTable (\n" +
                " ts BIGINT,\n" +
                " num INT ,\n" +
                " vin STRING ,\n" +
                " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')), \n " +
                " pts AS PROCTIME() ,  \n" +  //处理时间
                "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'tsp-vehicle-status-rt-bigdata',\n" +
                " 'properties.bootstrap.servers' = '10.210.100.17:9092',\n" +
                " 'properties.group.id' = 'x1',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset' \n" +
                ")";
        tableEnv.executeSql(kafkaSourceTable);

        String querySql = "SELECT * FROM kafkaSourceTable GROUP BY ts,num,vin,pts,rowtime, TUMBLE(rowtime, INTERVAL '5' SECOND)";
        final Table sourceTable = tableEnv.sqlQuery(querySql);

        sourceTable.printSchema();

        tableEnv.toAppendStream(sourceTable, Row.class).print();
        env.execute("xx");
    }
}
