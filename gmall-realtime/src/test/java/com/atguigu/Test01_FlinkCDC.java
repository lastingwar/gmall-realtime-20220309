package com.atguigu;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2022-08-15 10:19
 */
public class Test01_FlinkCDC {
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

        // TODO 3 使用flinkCDC读取配置表内容
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

        // TODO 4 输出流信息
        /*
    {
	"before": null,
	"after": {
		"source_table": "cart_info",
		"sink_table": "cartInfo",
		"sink_columns": null,
		"sink_pk": null,
		"sink_extend": null
	},
	"source": {
		"version": "1.5.4.Final",
		"connector": "mysql",
		"name": "mysql_binlog_source",
		"ts_ms": 1660530529516,
		"snapshot": "false",
		"db": "gmall_config",
		"sequence": null,
		"table": "table_process",
		"server_id": 0,
		"gtid": null,
		"file": "",
		"pos": 0,
		"row": 0,
		"thread": null,
		"query": null
	},
	"op": "r",
	"ts_ms": 1660530529519,
	"transaction": null
    }
         */
        tableConfigStream.print("table_config>>>>>>>>>");

        // TODO 5 执行任务
        env.execute();

    }
}
