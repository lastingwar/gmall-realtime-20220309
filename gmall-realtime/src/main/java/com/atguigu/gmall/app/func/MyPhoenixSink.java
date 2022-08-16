package com.atguigu.gmall.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.GmallConfig;
import com.atguigu.gmall.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author yhm
 * @create 2022-08-16 10:04
 */
public class MyPhoenixSink extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidPhoenixDSUtil.getDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context)  {
        // 拼接sql写入到phoenix
        String sql = createUpsertSQL(value);
//        System.out.println(sql);

        // 使用连接执行sql
        try {
            DruidPooledConnection connection = druidDataSource.getConnection();
            PhoenixUtil.executeSql(sql,connection);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("druid获取连接异常");
        }
    }

    private String createUpsertSQL(JSONObject jsonObject) {
        // upsert into db.sink_table (columns) values (值)
        String sinkTable = jsonObject.getString("sink_table");
        StringBuilder sql = new StringBuilder();
        jsonObject.remove("sink_table");
        Set<String> cols = jsonObject.keySet();
        Collection<Object> values = jsonObject.values();
        sql.append("upsert into ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(")
                .append(StringUtils.join(cols,","))
                .append(") values ('")
                .append(StringUtils.join(values,"','"))
                .append("')");

        return sql.toString();
    }
}
