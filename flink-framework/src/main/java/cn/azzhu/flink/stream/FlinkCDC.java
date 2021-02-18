package cn.azzhu.flink.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * FlinkCDC：MySql -》canal -》kafka -》Mysql
 * @author azzhu
 * @since 2021-02-18 23:06
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String createCanalKafaTable = "CREATE TABLE canal_kafka_table (\n" +
                "  id INT, \n" +
                "  username STRING, \n" +
                "  age INT, \n" +
                "  addr STRING \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'test_canal',\n" +
                " 'properties.bootstrap.servers' = 'node01:9092,node01:9092,node01:9092',\n" +
                " 'properties.group.id' = 'canalGroup',\n" +
                " 'format' = 'canal-json' \n" +
                ")";

        String sinkMysqlTable = "CREATE TABLE sink_mysql_table (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT, \n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://192.168.122.122:3306/canal_test',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root',\n" +
                "   'table-name' = 'mytest2'\n" +
                ")";


        String insert = "INSERT INTO sink_mysql_table SELECT id,username,age FROM canal_kafka_table";

        tableEnv.executeSql(createCanalKafaTable);
        tableEnv.executeSql(sinkMysqlTable);

        tableEnv.executeSql(insert);
    }
}
