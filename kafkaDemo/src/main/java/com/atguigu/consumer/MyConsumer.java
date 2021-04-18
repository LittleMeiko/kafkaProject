package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {
        //1.设置配置信息
        Properties properties = new Properties();
        //1.1.设置kafka集群信息
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "mq1:9092");
        //1.2.开启自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //1.3.设置自动提交的延时
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //1.4.设置反序列化类的key
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //1.5.设置反序列化类的value
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //1.6.设置消费者组id
        //properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");
        /*
         1.7.设置消费者的offset
            当换一个消费者组或者之前的消息过期被清除时，消费者可以消费到之前生产者发送的消息
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //2.创建consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //3.订阅主题
        consumer.subscribe(Arrays.asList("first", "second"));
        //4.消费消息
        while (true){
            //4.1.设置延时主动拉取消息
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for(ConsumerRecord<String,String> consumerRecord: consumerRecords){
                //4.2.获取消息
                System.out.println(consumerRecord.key() + "--" + consumerRecord.value());
            }
        }
    }
}
