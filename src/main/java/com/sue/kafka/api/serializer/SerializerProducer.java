package com.sue.kafka.api.serializer;

import com.alibaba.fastjson.JSON;
import com.sue.kafka.api.entity.User;
import com.sue.kafka.api.topic.Const;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author sue
 * @date 2020/8/17 15:21
 */

public class SerializerProducer {
    public static void main(String[] args){

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.182.150:9092");

        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"serial-producer");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,UserSerializer.class.getName());
        //创建kafkaProducer对象，传递properties属性参数集合
        KafkaProducer<String,User> producer = new KafkaProducer<String, User>(properties);
        //构造消息内容
        User user = new User("001","张三");
        ProducerRecord<String,User> record = new ProducerRecord<>(Const.TOPIC_SERIAL, user);
        //发送消息
        producer.send(record);
        //关闭生产者
        producer.close();
    }
}
