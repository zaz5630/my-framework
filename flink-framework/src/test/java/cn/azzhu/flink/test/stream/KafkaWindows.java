package cn.azzhu.flink.test.stream;


import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 处理kafka数据
 */
public class KafkaWindows {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String kafkaSourceTable = "CREATE TABLE kafkaSourceTable (\n" +
                " `msgtime` BIGINT,\n" +
                " `vin` STRING ,\n" +
                " `pts` AS PROCTIME() ,  \n" +  //处理时间
                " `rowtime` AS TO_TIMESTAMP(FROM_UNIXTIME(`msgtime` / 1000, 'yyyy-MM-dd HH:mm:ss')), \n " +
                " WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '2' SECOND \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'topic',\n" +
                " 'properties.bootstrap.servers' = 'ip:9092',\n" +
                " 'properties.group.id' = 'mmx',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset' \n" +
                ")";
        tableEnv.executeSql(kafkaSourceTable);

        String queryWindowAllDataSql = "SELECT max(msgtime),TUMBLE_START(rowtime, INTERVAL '5' SECOND),TUMBLE_END(rowtime, INTERVAL '5' SECOND) " +
                " from kafkaSourceTable  group by TUMBLE(rowtime, INTERVAL '5' SECOND)";
//        String querysql = "select max(`num`) from kafkaSourceTable group by TUMBLE(`rowtime`, INTERVAL '5' SECOND)";

        String querysql = "select max(msgtime) from kafkaSourceTable group by TUMBLE(rowtime,INTERVAL '5' SECOND)";
        final Table windowAllTable = tableEnv.sqlQuery(querysql);

        windowAllTable.printSchema();
        tableEnv.toAppendStream(windowAllTable, Row.class).print();
        System.out.println("------------------------------------------------------");
        env.execute("job");

    }

}
