package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private int success;
    private int error;

    public void configure(Map<String, ?> map) {

    }

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    // 记录消息发送成功的次数、失败的次数
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (recordMetadata != null){
            success++;
        }else{
            error++;
        }
    }

    public void close() {
        System.out.println("success:" + success);
        System.out.println("error:" + error);
    }

}
