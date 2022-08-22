package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author yhm
 * @create 2022-08-22 10:22
 */
public class DwdTradeOrderPreProcess {
    public static void main(String[] args)  {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905L));

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 读取kafka_db中的数据
        String topicName = "topic_db";
        String groupID = "dwd_trade_order_pre_process";
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

        // TODO 4 过滤出订单详情表
        Table orderDetailTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id,\n" +
                "  `data`['order_id'] order_id,\n" +
                "  `data`['sku_id'] sku_id,\n" +
                "  `data`['sku_name'] sku_name,\n" +
                "  `data`['order_price'] order_price,\n" +
                "  `data`['sku_num'] sku_num,\n" +
                "  `data`['create_time'] create_time,\n" +
                "  `data`['source_type'] source_type,\n" +
                "  `data`['source_id'] source_id,\n" +
                "  `data`['split_total_amount'] split_total_amount,\n" +
                "  `data`['split_activity_amount'] split_activity_amount,\n" +
                "  `data`['split_coupon_amount'] split_coupon_amount,\n" +
                "  cast (cast(`data`['order_price'] as decimal(16,2)) * \n" +
                "  cast(`data`['sku_num'] as decimal(16,2)) as string) split_original_amount ,\n" +
                "  `pt`,\n" +
                "  `ts`\n" +
                "from topic_db\n" +
                "where `table`='order_detail'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail",orderDetailTable);


        // TODO 5 过滤出订单表
        Table orderInfoTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id,\n" +
                "  `data`['order_status'] order_status,\n" +
                "  `data`['user_id'] user_id,\n" +
                "  `data`['operate_time'] operate_time,\n" +
                "  `data`['province_id'] province_id,\n" +
                "   `type`,\n" +
                "   `old`,\n" +
                "  `data`['province_id'] province_id,\n" +
                "  `ts`,\n" +
                "  `pt`\n" +
                "from topic_db\n" +
                "where`table`='order_info'\n" +
                "and (`type`='insert' or `type`='update')");

        tableEnv.createTemporaryView("order_info",orderInfoTable);

        // TODO 6 过滤出订单详情活动表
        Table actTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] order_detail_id,\n" +
                "  `data`['activity_id'] activity_id,\n" +
                "  `data`['activity_rule_id'] activity_rule_id,\n" +
                "  `pt`,\n" +
                "  `ts`\n" +
                "from topic_db\n" +
                "where `table`='order_detail_activity'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity",actTable);

        // TODO 7 过滤出订单详情优惠券表
        Table couTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] order_detail_id, \n" +
                "  `data`['coupon_id'] coupon_id, \n" +
                "  `data`['coupon_use_id'] coupon_use_id, \n" +
                "  `pt`,\n" +
                "  `ts`\n" +
                "from topic_db\n" +
                "where `table`='order_detail_coupon'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_coupon",couTable);

        // TODO 8 读取mysql中的base_dic表格
        tableEnv.executeSql(MysqlUtil.getBaseDicDDL());

        // TODO 9 join读取的5张表格
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "  od.id,\n" +
                "  od.order_id,\n" +
                "  od.sku_id,\n" +
                "  od.sku_name,\n" +
                "  od.order_price,\n" +
                "  od.sku_num,\n" +
                "  od.create_time,\n" +
                "  b.dic_name source_type,\n" +
                "  od.source_id,\n" +
                "  od.split_total_amount,\n" +
                "  od.split_activity_amount,\n" +
                "  od.split_coupon_amount,\n" +
                "  od.split_original_amount,\n" +
                "  od.ts od_ts,\n" +
                "  oi.order_status,\n" +
                "  oi.user_id,\n" +
                "  oi.operate_time,\n" +
                "  oi.province_id,\n" +
                "  oi.ts oi_ts,\n" +
                "  oi.pt,\n" +
                "  act.activity_id,\n" +
                "  act.activity_rule_id,\n" +
                "  cou.coupon_id,\n" +
                "  cou.coupon_use_id,\n" +
                "   oi.`type`,\n" +
                "   oi.`old`,\n" +
                "  current_row_timestamp() row_op_ts\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id=oi.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id=act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id=cou.order_detail_id\n" +
                "join base_dic FOR SYSTEM_TIME AS OF oi.pt AS b\n" +
                "on b.dic_code=od.source_type");
        tableEnv.createTemporaryView("result_table",resultTable);

        // TODO 10 写入到kafka新的主题中dwd_trade_order_pre_process
        tableEnv.executeSql("create table kafka_sink(\n" +
                "  id string,\n" +
                "  order_id string,\n" +
                "  sku_id string,\n" +
                "  sku_name string,\n" +
                "  order_price string,\n" +
                "  sku_num string,\n" +
                "  create_time string,\n" +
                "  source_type string,\n" +
                "  source_id string,\n" +
                "  split_total_amount string,\n" +
                "  split_activity_amount string,\n" +
                "  split_coupon_amount string,\n" +
                "  split_original_amount string,\n" +
                "  od_ts bigint,\n" +
                "  order_status string,\n" +
                "  user_id string,\n" +
                "  operate_time string,\n" +
                "  province_id string,\n" +
                "  oi_ts bigint,\n" +
                "  pt TIMESTAMP_LTZ(3),\n" +
                "  activity_id string,\n" +
                "  activity_rule_id string,\n" +
                "  coupon_id string,\n" +
                "  coupon_use_id string,\n" +
                "   `type` string,\n" +
                "   `old` map<string,string>,\n" +
                "  row_op_ts TIMESTAMP_LTZ(3),\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + KafkaUtil.getUpsertKafkaSinkDDL("dwd_trade_order_pre_process"));

        tableEnv.executeSql("insert into kafka_sink select * from result_table");


    }
}
