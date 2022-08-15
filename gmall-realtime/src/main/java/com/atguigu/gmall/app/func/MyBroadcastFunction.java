package com.atguigu.gmall.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2022-08-15 11:30
 */
public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {


    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 1 判断类型  如果是删除  从状态中去除对应的表

        // 2 判断表格是否在phoenix中存在  不存在创建

        // 3 将内容写入状态
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1 获取状态  判断当前表是否为维度表  不是删除

        // 2 过滤掉多余的字段  只保留sink_columns字段

        // 3 添加sink_table字段
    }


}
