package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建kafka生产者的配置信息
        Properties properties = new Properties();
        //2.kafka 集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "mq1:9092");
        //3.ack应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //4.重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        //5.批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //6.等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //7.RecordAccumulator 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //8.序列化器的类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //9.生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //10.发送消息
        for (int i=0;i < 10;i++){
            //send()的返回值Future<RecordMetadata>调用get()即可变为同步发送
            //producer.send(new ProducerRecord<String, String>("first","atguigu---" + i)).get();
            //异步发送
            producer.send(new ProducerRecord<String, String>("first","atguigu---" + i));
        }
        //11.关闭资源
        producer.close();
    }
}
