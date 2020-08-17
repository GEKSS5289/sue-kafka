package com.sue.kafka.api.partition;

import com.alibaba.fastjson.JSON;
import com.sue.kafka.api.entity.User;
import com.sue.kafka.api.partition.custom.CustomPartition;
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

public class PartitionProducer {
    public static void main(String[] args){

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.182.150:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"partition-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartition.class.getName());




        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        //构造消息内容
        User user = new User("001","张三");
        ProducerRecord<String,String> record = new ProducerRecord<>(Const.TOPIC_PARTITION, JSON.toJSONString(user));
        //发送消息
        producer.send(record);
        //关闭生产者
        producer.close();
    }
}
