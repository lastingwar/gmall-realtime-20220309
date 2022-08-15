package com.atguigu.gmall.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TableProcess;
import com.atguigu.gmall.util.KafkaUtil;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yhm
 * @create 2022-08-13 15:20
 */
public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        // TODO 2 设置检查点和状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");
        env.setStateBackend(new HashMapStateBackend());
         */

        // TODO 3 读取kafka对应的数据
        String topicName = "topic_db";
        String groupID = "dim_sink_app";
        DataStreamSource<String> topicDbStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));

        topicDbStream.print("topic_db>>>>>>>>");

        // TODO 4 转换格式和清洗过滤脏数据
        //不是json的数据；
        //type类型为bootstrap-start和bootstrap-complete
        /*
        SingleOutputStreamOperator<JSONObject> jsonObjStream = topicDbStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (!"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                        out.collect(jsonObject);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        });
         */

        // 将脏数据写入到侧输出流中
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjStream = topicDbStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (!"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                        out.collect(jsonObject);
                    } else {
                        // 特殊类型没有意义的数据 写到侧输出流
                        ctx.output(dirtyOutputTag, value);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    // 不为json写到侧输出流
                    ctx.output(dirtyOutputTag, value);
                }
            }
        });

        // 获取脏数据的流
        DataStream<String> dirtyStream = jsonObjStream.getSideOutput(dirtyOutputTag);

//        dirtyStream.print("dirty>>>>>>>>>>");

        // TODO 5 使用flinkCDC读取配置表数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_config")
                // 坑 需要填写库名加表名
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 数据输出格式
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> tableConfigStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "table_config");

        // TODO 6 将配置流转换为广播流和主流进行连接
        // K String(表名)  判断当前表是否为维度表
        // V (后面的数据)   能够完成在phoenix中创建表格的工作
        BroadcastStream<String> broadcastStream = tableConfigStream.broadcast(new MapStateDescriptor<String, TableProcess>("table_process",String.class,TableProcess.class));


        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjStream.connect(broadcastStream);

        // TODO 7 处理连接流 根据配置流的信息  过滤出主流的维度表内容
        connectedStream.process(new BroadcastProcessFunction<JSONObject, String, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

            }
        });

        // TODO 8 将数据写入到phoenix中


        // TODO 9 执行任务
        env.execute(groupID);

    }
}
