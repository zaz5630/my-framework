package cn.azzhu.flink.test.batch;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Mysql数据 写入 Hive
 */
public class MysqlToHive {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String createMysqlTableSQL = "CREATE TABLE mysqlTable1 (\n" +
                "  id INT,\n" +
                "  username STRING,\n" +
                "  age INT , \n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc' ,\n" +
                "   'url' = 'jdbc:mysql://IP:3306/bigdata',\n" +
                "   'table-name' = 'test' ,\n" +
                "   'username' = 'root' ,\n" +
                "   'password' = 'root' \n" +
                ")";
        String  print_table= " CREATE TABLE print_table (" +
                "         id INT," +
                "         username STRING," +
                "         age INT" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        tableEnv.executeSql(createMysqlTableSQL);
        tableEnv.executeSql(print_table);

        String querysql = "insert into print_table select * from mysqlTable1";

        tableEnv.executeSql(querysql);
    }
}
