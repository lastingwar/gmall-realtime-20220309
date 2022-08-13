package com.atguigu.gmall.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.KafkaUtil;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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


        // TODO  执行任务
        env.execute(groupID);

    }
}
