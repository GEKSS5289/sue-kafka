package com.sue.kafka.api.interceptor;

import com.alibaba.fastjson.JSON;
import com.sue.kafka.api.entity.User;
import com.sue.kafka.api.interceptor.custom.CustomProducerInterceptor;
import com.sue.kafka.api.topic.Const;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author sue
 * @date 2020/8/17 11:01
 */

public class InterceptorProducer {
    public static void main(String[] args){

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.182.150:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"interceptor-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomProducerInterceptor.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        System.out.println(CustomProducerInterceptor.class.getName());
        User user = new User("001","张三");
        ProducerRecord<String,String> record = new ProducerRecord<>(Const.TOPIC_INTERCEPTOR, JSON.toJSONString(user));

        producer.send(record);

        producer.close();
    }
}
