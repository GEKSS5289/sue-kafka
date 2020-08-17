package com.sue.kafka.api.serializer;

import com.sue.kafka.api.topic.Const;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author sue
 * @date 2020/8/17 15:21
 */

public class DeserializerConsumer {
    public static void main(String[] args){
        //配置属性参数
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.182.150:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //非常重要的属性配置 与我们消费者订阅组有关系
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"serial-group");
        //常规属性
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,10000);
        //消费者提交offset：自动提交 & 手动提交 默认是自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,5000);

        //创建消费者对象
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        //订阅你感兴趣的主题
        consumer.subscribe(Collections.singletonList(Const.TOPIC_SERIAL));
        System.err.println("quickstart consumer started...");
        try {
            //采用拉取的方式消费数据
            while(true){
                //等待多久拉取一次消息
                //拉取TOPIC_QUICKSTART主题里面的所有消息
                //topic 和 partition是一对多关系 一个topic可以用多个partition
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                //因为partition中存储的，所以需要遍历partition集合
                poll.partitions().forEach(topicPartition -> {

                    //通过TopicPartition获取到实际records对象中数据集合
                    List<ConsumerRecord<String, String>> records = poll.records(topicPartition);
                    //获取当前topicPartition消息条数
                    int size = records.size();
                    //获取到TopicPartition对应的主题名称
                    String topic = topicPartition.topic();
                    System.out.println(String.format("---获取topic:%s,分区位置:%s,消息总数:%s",
                            topic,
                            topicPartition.partition(),
                            size
                    ));


                    for(int i = 0;i<size;i++){
                        //实际数据
                        ConsumerRecord<String, String> consumerRecord = records.get(i);
                        String value = consumerRecord.value();
                        long offset = consumerRecord.offset();
                        long commitOffset = offset+1;
                        System.out.println(String.format("获取实际消息value:%s,消息offset:%s,提交offset:%s",value,offset,commitOffset));
                    }
                });
            }
        }finally {
            consumer.close();
        }


    }
}
