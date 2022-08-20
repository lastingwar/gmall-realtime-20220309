package com.atguigu;

import com.atguigu.bean.NameBean;
import com.atguigu.bean.SexBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2022-08-20 9:21
 */
public class Test03_IntervalJoin {
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

        // 添加水位线使用事件时间
        SingleOutputStreamOperator<NameBean> nameBeanWatermarkStream = nameBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<NameBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<NameBean>() {
            @Override
            public long extractTimestamp(NameBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        SingleOutputStreamOperator<SexBean> sexBeanWatermarkStream = sexBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<SexBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<SexBean>() {
            @Override
            public long extractTimestamp(SexBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 使用intervalJoin
        SingleOutputStreamOperator<Tuple2<NameBean, SexBean>> processStream = nameBeanWatermarkStream.keyBy(new KeySelector<NameBean, String>() {
            @Override
            public String getKey(NameBean value) throws Exception {
                return value.getId();
            }
        }).intervalJoin(sexBeanWatermarkStream.keyBy(new KeySelector<SexBean, String>() {
            @Override
            public String getKey(SexBean value) throws Exception {
                return value.getId();
            }
        })).between(Time.seconds(-3L), Time.seconds(3L))
                .process(new ProcessJoinFunction<NameBean, SexBean, Tuple2<NameBean, SexBean>>() {
                    @Override
                    public void processElement(NameBean left, SexBean right, Context ctx, Collector<Tuple2<NameBean, SexBean>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        // TODO 6 打印数据
        processStream.print(">>>>>");

        // TODO 7 执行任务
        env.execute();

    }
}
