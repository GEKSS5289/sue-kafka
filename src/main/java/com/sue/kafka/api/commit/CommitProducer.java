package com.sue.kafka.api. commit;

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
 * @date 2020/8/17 11:01
 */

public class CommitProducer {
    public static void main(String[] args){
        //配置生产者启动关键数据
        Properties properties = new Properties();
        //BOOTSTRAP_SERVERS_CONFIG:连接kafka集群服务列表，如果有多个用逗号分隔
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.182.150:9092");
        //CLIENT_ID_CONFIG:这个属性标记kafkaclient的ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"core-producer");
        //对kafka的key以及value做序列化
        //key:是kafka用于做消息投递计算具体投递到对应的主题的哪一个partition而需要
        //value:实际发送的内容
        //为什么需要序列化? 因为我们kafkaBroker在接收消息的时候必须要以二进制的方式接收，所以需要序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //创建kafkaProducer对象，传递properties属性参数集合
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        //构造消息内容
        User user = new User("001","张三");
        ProducerRecord<String,String> record = new ProducerRecord<>(Const.TOPIC_CORE, JSON.toJSONString(user));
        //发送消息
        producer.send(record);
        //关闭生产者
        producer.close();
    }
}
