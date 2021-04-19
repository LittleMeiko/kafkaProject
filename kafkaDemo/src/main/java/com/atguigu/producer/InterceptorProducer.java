package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class InterceptorProducer {

    public static void main(String[] args) {
        //1.创建并设置kafka生产者的配置信息
        Properties properties = new Properties();
        //1.1.设置kafka集群信息，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "mq1:9092");
        //1.2.设置应答级别ack
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //1.3.设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        //1.4.设置批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //1.5.设置等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //1.6.设置RecordAccumulator缓冲区的大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //1.7.设置序列化器类的key
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //1.8.设置序列化器类的value
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //1.9.设置使用的拦截器
        ArrayList<String> interceptors = new ArrayList<String>();
        interceptors.add("com.atguigu.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        //2.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //3.发送消息
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", "atguigu---" + i));
        }

        //4.关闭资源
        producer.close();
    }
}
