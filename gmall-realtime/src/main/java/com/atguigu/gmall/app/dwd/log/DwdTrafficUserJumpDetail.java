package com.atguigu.gmall.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author yhm
 * @create 2022-08-19 15:15
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 读取kafka对应主题page数据
        String page_topic = "dwd_traffic_page_log";
        String groupID = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupID));

        // TODO 4 转换数据结构为jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageStream.map(JSON::parseObject);

        // TODO 5 添加水位线watermark
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 6 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 7 定义一个匹配规则Pattern
        Pattern<JSONObject, JSONObject> objectPattern = Pattern.<JSONObject>begin("begin")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                        // 一个会话的第一条数据
                        String lastPageID = value.getJSONObject("page").getString("last_page_id");
                        return lastPageID == null;
                    }
                })
                .next("next")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                        // 下一个会话的第一条数据
                        String lastPageID = value.getJSONObject("page").getString("last_page_id");
                        return lastPageID == null;
                    }
                }).within(Time.seconds(15L));


        // TODO 8 使用规则对数据流进行匹配
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, objectPattern);

        // TODO 9 拆分出超时流
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeout"){};
        SingleOutputStreamOperator<JSONObject> selectStream = patternStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<JSONObject> out) throws Exception {
                List<JSONObject> begin = pattern.get("begin");
                JSONObject jsonObject = begin.get(0);
                out.collect(jsonObject);
            }
        }, new PatternFlatSelectFunction<JSONObject, JSONObject>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {
                List<JSONObject> begin = pattern.get("begin");
                JSONObject jsonObject = begin.get(0);
                out.collect(jsonObject);
            }
        });

        /*
        patternStream.select(timeoutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                List<JSONObject> begin = pattern.get("begin");
                JSONObject jsonObject = begin.get(0);
                return jsonObject;
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                List<JSONObject> begin = pattern.get("begin");
                JSONObject jsonObject = begin.get(0);
                return jsonObject;
            }
        });
         */


        DataStream<JSONObject> timeoutStream = selectStream.getSideOutput(timeoutTag);

        // TODO 10 合并两个流
        DataStream<JSONObject> jumpStream = selectStream.union(timeoutStream);

        // TODO 11 写出到新的kafka主题
        String targetTopic = "dwd_traffic_user_jump_detail";
        jumpStream.map(JSONAware::toJSONString)
                .addSink(KafkaUtil.getKafkaProducer(targetTopic));

        jumpStream.print("jump>>>");

        // TODO 12 执行任务
        env.execute(groupID);

    }
}
