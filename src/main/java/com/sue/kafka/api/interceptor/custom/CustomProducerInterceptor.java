package com.sue.kafka.api.interceptor.custom;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author sue
 * @date 2020/8/17 14:48
 */

public class CustomProducerInterceptor implements ProducerInterceptor<String,String> {

    private volatile long success = 0;
    private volatile long failure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        System.out.println("-------生产者前置拦截器------");
        String value = "prefix-"+ producerRecord.value();
        return new ProducerRecord<String,String>(producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.timestamp(),
                producerRecord.key(),value,producerRecord.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.out.println("生产者发送消息后置拦截器");
        if(e == null){
            success ++;
        }else{
            failure++;
        }
    }

    @Override
    public void close() {
        System.out.println(String.format("生产者关闭，发送消息成功率为:%s %%",(double)success/(success+failure)));
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
