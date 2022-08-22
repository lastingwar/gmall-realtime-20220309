package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2022-08-22 14:42
 */
public class DwdTradeCancelDetail {
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

        // TODO 3 读取kafka的主题数据订单预处理
        String topicName = "dwd_trade_order_pre_process";
        String groupID = "dwd_trade_cancel_detail";
        tableEnv.executeSql("create table order_pre(\n" +
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
                "  row_op_ts TIMESTAMP_LTZ(3) \n" +
                ")" + KafkaUtil.getKafkaDDL(topicName,groupID));

        // TODO 4 过滤出取消订单数据
        Table filterTable = tableEnv.sqlQuery("select \n" +
                "  id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  create_time,\n" +
                "  source_type,\n" +
                "  source_id,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  split_original_amount,\n" +
                "  od_ts,\n" +
                "  order_status,\n" +
                "  user_id,\n" +
                "  operate_time,\n" +
                "  province_id,\n" +
                "  oi_ts,\n" +
                "  pt,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  coupon_use_id,\n" +
                "  row_op_ts\n" +
                "from order_pre\n" +
                "where type = 'update'\n" +
                "and order_status='1003'");
        tableEnv.createTemporaryView("filter_table",filterTable);

        // TODO 5 写出到kafka新的主题中
        tableEnv.executeSql("create table cancel_detail(\n" +
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
                "  row_op_ts TIMESTAMP_LTZ(3),\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + KafkaUtil.getUpsertKafkaSinkDDL("dwd_trade_cancel_detail"));

        tableEnv.executeSql("insert into cancel_detail select * from filter_table");
    }
}
