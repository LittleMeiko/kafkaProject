package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class PartitionerProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "mq1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 使用自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.partitioner.MyPartitioner");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i=0;i < 10;i++){
            producer.send(new ProducerRecord<String, String>("first", "atguigu" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        System.out.println(recordMetadata.partition() + "--" + recordMetadata.offset());
                    }else{
                        e.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }
}
