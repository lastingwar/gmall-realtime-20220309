package com.atguigu.gmall.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yhm
 * @create 2022-08-16 14:04
 */
public class BaseLogApp {
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

        // TODO 3 读取kafka主题topic_log中的数据
        String topicName = "topic_log";
        String groupID = "base_log_app";
        DataStreamSource<String> topicLogStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));

        // TODO 4 清洗转换数据
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjStream = topicLogStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (JSONException e) {
                    e.printStackTrace();
                    ctx.output(dirtyOutputTag, value);
                }
            }
        });

        // TODO 5 根据mid分组数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        // TODO 6 根据键控状态完成新旧访客的标记修复
        SingleOutputStreamOperator<JSONObject> isNewStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            // 状态的格式  yyyy-MM-dd
            private ValueState<String> firstVisitDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_visit_dt", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");
                String firstVisitDt = firstVisitDtState.value();
                String visitDt = DateFormatUtil.toDate(value.getLong("ts"));
                if ("1".equals(isNew)) {
                    if (firstVisitDt == null) {
                        // 没有登录过的新访客
                        firstVisitDtState.update(visitDt);
                    } else if (!firstVisitDt.equals(visitDt)) {
                        // 删除了自己APP的旧访客  不是新访客
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else {
                    // 标记为0
                    if (firstVisitDt == null) {
                        // 修改状态日期为前一天
                        String yesterday = DateFormatUtil.toDate(value.getLong("ts") - 1000L * 60 * 60 * 24);
                        firstVisitDtState.update(yesterday);
//                        System.out.println(yesterday);
                    }
                }
                return value;
            }
        });

//        isNewStream.print("isNew>>>>>>>>");

        // TODO 7 将数据拆分为5种数据流
        // page页面当做主流  另外4种 start  err  action display作为侧输出流
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> errOutputTag = new OutputTag<String>("err") {
        };
        OutputTag<String> actionOutputTag = new OutputTag<String>("action") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageStream = isNewStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                // topic_log中有两种日志  start  page
                // 1. 错误数据输出到侧输出流中
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errOutputTag, err);
                }
                value.remove("err");

                // 2. 启动日志输出到侧输出流
                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    // 当前为启动日志  将全部数据写出到start侧输出流
                    ctx.output(startOutputTag, value.toJSONString());
                } else {
                    // 当前页面日志
                    // 获取公共信息
                    JSONObject common = value.getJSONObject("common");
                    Long ts = value.getLong("ts");
                    JSONObject page = value.getJSONObject("page");

                    // 3. 打散输出action数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            ctx.output(actionOutputTag, action.toJSONString());
                        }
                    }
                    value.remove("actions");

                    // 4. 打散输出display数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            ctx.output(displayOutputTag, display.toJSONString());
                        }
                    }
                    value.remove("displays");

                    // 5. 主流输出page数据
                    out.collect(value.toJSONString());
                }
            }
        });

        DataStream<String> startStream = pageStream.getSideOutput(startOutputTag);
        DataStream<String> errStream = pageStream.getSideOutput(errOutputTag);
        DataStream<String> actionStream = pageStream.getSideOutput(actionOutputTag);
        DataStream<String> displayStream = pageStream.getSideOutput(displayOutputTag);

        pageStream.print("page>>>>>>>>");
        startStream.print("start>>>>>>>>>");
        errStream.print("err>>>>>>>>>");
        actionStream.print("action>>>>>>>>>");
        displayStream.print("display>>>>>>>>>");

        // TODO 8 将5种数据流写回到对应的kafka主题
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageStream.addSink(KafkaUtil.getKafkaProducer(page_topic));
        startStream.addSink(KafkaUtil.getKafkaProducer(start_topic));
        displayStream.addSink(KafkaUtil.getKafkaProducer(display_topic));
        actionStream.addSink(KafkaUtil.getKafkaProducer(action_topic));
        errStream.addSink(KafkaUtil.getKafkaProducer(error_topic));

        // TODO 9 执行任务
        env.execute(groupID);
    }
}
