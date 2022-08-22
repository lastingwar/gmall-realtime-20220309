package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.checkerframework.checker.units.qual.K;

/**
 * @author yhm
 * @create 2022-08-22 15:22
 */
public class DwdTradePayDetailSuc {
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

        // TODO 3 读取kafka的topic_db数据
        String topicName = "topic_db";
        String groupID = "dwd_trade_pay_detail_suc";
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

        // TODO 4 过滤出支付成功数据
        Table filterTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id , \n" +
                "  `data`['order_id'] order_id , \n" +
                "  `data`['payment_type'] payment_type , \n" +
                "  `data`['create_time'] create_time \n" +
                "from topic_db\n" +
                "where `table`='payment_info'\n" +
                "and type='update'\n" +
                "and data['payment_status']='1602'");
        tableEnv.createTemporaryView("payment_info",filterTable);

        // TODO 5 读取order_detail数据
        tableEnv.executeSql("create table order_detail(\n" +
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
                "  row_op_ts TIMESTAMP_LTZ(3)\n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_trade_order_detail",groupID));

        // TODO 6 读取mysql的base_dic数据
        tableEnv.executeSql(MysqlUtil.getBaseDicDDL());

        // TODO 7 进行join操作
        Table payTable = tableEnv.sqlQuery("select \n" +
                "  od.id, \n" +
                "  od.order_id, \n" +
                "  od.sku_id, \n" +
                "  od.sku_name, \n" +
                "  od.order_price, \n" +
                "  od.sku_num, \n" +
                "  od.create_time, \n" +
                "  od.source_type, \n" +
                "  od.source_id, \n" +
                "  od.split_total_amount, \n" +
                "  od.split_activity_amount, \n" +
                "  od.split_coupon_amount, \n" +
                "  od.split_original_amount, \n" +
                "  od.od_ts, \n" +
                "  od.order_status, \n" +
                "  od.user_id, \n" +
                "  od.operate_time, \n" +
                "  od.province_id, \n" +
                "  od.oi_ts, \n" +
                "  od.pt, \n" +
                "  od.activity_id, \n" +
                "  od.activity_rule_id, \n" +
                "  od.coupon_id, \n" +
                "  od.coupon_use_id, \n" +
                "  od.row_op_ts, \n" +
                "  b.dic_name payment_type, \n" +
                "  pi.create_time pay_time \n" +
                "from payment_info pi\n" +
                "join order_detail od\n" +
                "on pi.order_id=od.order_id\n" +
                "join base_dic b\n" +
                "on pi.payment_type=b.dic_code");
        tableEnv.createTemporaryView("pay_table",payTable);


        // TODO 8 写出到新的kafka主题中
        tableEnv.executeSql("create table pay_success(\n" +
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
                "  payment_type string,\n" +
                "  pay_time string ,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + KafkaUtil.getUpsertKafkaSinkDDL("dwd_trade_pay_detail_suc"));

        tableEnv.executeSql("insert into pay_success select * from pay_table");

    }
}
