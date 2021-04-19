package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String,String> {

    public void configure(Map<String, ?> map) {

    }

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 获取初始的数据，并在前面加上时间戳后发出去
        return new ProducerRecord<String, String>(producerRecord.topic(),producerRecord.partition(),
                producerRecord.key(),System.currentTimeMillis() + "," + producerRecord.value());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }
}
