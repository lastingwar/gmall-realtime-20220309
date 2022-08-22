package com.atguigu;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author yhm
 * @create 2022-08-22 11:49
 */
public class CustomConsumer {
    public static void main(String[] args) {
        //1. 创建配置对象
        Properties properties = new Properties();

        // 2. 添加参数到配置对象
        // 必要参数  有4个
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");


        // 3.创建消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 注册主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("user_table");
        kafkaConsumer.subscribe(topics);

        while (true){
            // 4. 调用方法消费数据
            // 如果kafka集群没有新的数据  会造成空转
            // 填写参数为时间  如果没有拉取到数据  线程睡眠一会
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

            // 打印消费的数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "-"+consumerRecord.value() );
            }
        }

        // 5. 关闭资源
//        kafkaConsumer.close();
    }
}
