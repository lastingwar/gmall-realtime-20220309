package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2022-08-20 14:04
 */
public class DwdTradeCartAdd {
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

        // TODO 3 读取kafka对应主题topic_db数据
        String topicName = "topic_db";
        String groupID = "dwd_trade_cart_add";
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `ts` bigint,\n" +
                "  `xid` bigint,\n" +
                "  `commit` string,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string>, \n" +
                "   pt AS PROCTIME() \n" +
                ") " + KafkaUtil.getKafkaDDL(topicName, groupID));

        // TODO 4 过滤出加购的数据
        Table cartAddTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id,\n" +
                "  `data`['user_id'] user_id,\n" +
                "  `data`['sku_id'] sku_id,\n" +
                "  `data`['cart_price'] cart_price,\n" +
                "  if(`type` = 'insert',`data`['sku_num'],cast((cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint)) as String) \n" +
                "  ) sku_num,\n" +
                "  `data`['sku_name'] sku_name,\n" +
                "  `data`['is_checked'] is_checked,\n" +
                "  `data`['create_time'] create_time,\n" +
                "  `data`['operate_time'] operate_time,\n" +
                "  `data`['is_ordered'] is_ordered,\n" +
                "  `data`['order_time'] order_time,\n" +
                "  `data`['source_type'] source_type,\n" +
                "  `data`['source_id'] source_id,\n" +
                "   pt \n" +
                "from topic_db\n" +
                "where `table` = 'cart_info' \n" +
                "and (`type` = 'insert' or \n" +
                "  (`type` = 'update' and (\n" +
                "    cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint))> 0\n" +
                "     )\n" +
                "  )");
        tableEnv.createTemporaryView("cartAdd", cartAddTable);

        // TODO 5 读取mysql中的base_dic数据
        tableEnv.executeSql(MysqlUtil.getBaseDicDDL());

        // TODO 6 两张表进行lookUp join
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "  c.`id`,\n" +
                "  c.`user_id`,\n" +
                "  c.`sku_id`,\n" +
                "  c.`cart_price`,\n" +
                "  c.`sku_num`,\n" +
                "  c.`sku_name`,\n" +
                "  c.`is_checked`,\n" +
                "  c.`create_time`,\n" +
                "  c.`operate_time`,\n" +
                "  c.`is_ordered`,\n" +
                "  c.`order_time`,\n" +
                "  b.`dic_name`,\n" +
                "  c.`source_id`\n" +
                "from cartAdd as c\n" +
                "join base_dic FOR SYSTEM_TIME AS OF c.pt AS b\n" +
                "on c.source_type = b.dic_code");

        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7 写出到kafka中
        tableEnv.executeSql("create table kafka_sink(\n" +
                "  `id` string,\n" +
                "  `user_id` string,\n" +
                "  `sku_id` string,\n" +
                "  `cart_price` string,\n" +
                "  `sku_num` string,\n" +
                "  `sku_name` string,\n" +
                "  `is_checked` string,\n" +
                "  `create_time` string,\n" +
                "  `operate_time` string,\n" +
                "  `is_ordered` string,\n" +
                "  `order_time` string,\n" +
                "  `dic_name` string,\n" +
                "  `source_id` string \n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));

        tableEnv.executeSql("insert into kafka_sink select * from result_table");


    }
}
