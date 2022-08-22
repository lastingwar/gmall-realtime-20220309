package com.atguigu;

import com.atguigu.bean.NameBean;
import com.atguigu.bean.SexBean;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2022-08-22 11:35
 */
public class Test08_KafkaDDL1 {
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
        // TODO 3 读取节点数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("hadoop102", 9999);

        // TODO 4 过滤加转换
        SingleOutputStreamOperator<NameBean> nameBeanStream = dataStreamSource.flatMap(new FlatMapFunction<String, NameBean>() {
            @Override
            public void flatMap(String value, Collector<NameBean> out) throws Exception {
                try {
                    String[] data = value.split(",");
                    out.collect(new NameBean(data[0], data[1], Long.parseLong(data[2])));
                } catch (Exception e) {
                    System.out.println("数据出错");
                }
            }
        });

        SingleOutputStreamOperator<SexBean> sexBeanStream = dataStreamSource1.flatMap(new FlatMapFunction<String, SexBean>() {
            @Override
            public void flatMap(String value, Collector<SexBean> out) throws Exception {
                try {
                    String[] data = value.split(",");
                    out.collect(new SexBean(data[0], data[1], Long.parseLong(data[2])));
                } catch (Exception e) {
                    System.out.println("数据出错");
                }
            }
        });


        // TODO 5 转换流为表
        tableEnv.createTemporaryView("nameTable",nameBeanStream);
        tableEnv.createTemporaryView("sexTable",sexBeanStream);


        // TODO 6 join处理并输出
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "  n.id,\n" +
                "  n.name,\n" +
                "  s.sex\n" +
                "from nameTable n\n" +
                "left join sexTable s\n" +
                "on n.id = s.id");
        tableEnv.createTemporaryView("resultTable",resultTable);

        // TODO 7 写入数据到kafka
        tableEnv.executeSql("create table userTable(\n" +
                "  id string,\n" +
                "  name string,\n" +
                "  sex string, \n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + KafkaUtil.getUpsertKafkaSinkDDL("user_table"));

        tableEnv.executeSql("insert into userTable select * from resultTable");
    }
}
