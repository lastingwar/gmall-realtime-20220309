package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2022-08-20 11:19
 */
public class Test05_LookupJoin {
    public static void main(String[] args) {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        tableEnv.executeSql("CREATE TEMPORARY TABLE base_dic (\n" +
                "  `dic_code` string,\n" +
                "  `dic_name` string,\n" +
                "  `parent_code` string,\n" +
                "  `create_time` string,\n" +
                "  `operate_time` string\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', \n" +
                "  'table-name' = 'base_dic'\n" +
                ")");

        tableEnv.sqlQuery("select * from base_dic").execute().print();


    }
}
