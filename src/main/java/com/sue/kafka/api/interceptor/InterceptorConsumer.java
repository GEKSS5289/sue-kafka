package com.sue.kafka.api.interceptor;

import com.sue.kafka.api.interceptor.custom.CustomConsumerInterceptor;
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
 * @date 2020/8/17 11:01
 */

public class InterceptorConsumer {
    public static void main(String[] args){

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.182.150:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"interceptor-group");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,10000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,5000);
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomConsumerInterceptor.class.getName());
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(Const.TOPIC_INTERCEPTOR));
        System.err.println("quickstart consumer started...");
        try {
            while(true){
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                poll.partitions().forEach(topicPartition -> {
                    List<ConsumerRecord<String, String>> records = poll.records(topicPartition);
                    int size = records.size();
                    String topic = topicPartition.topic();
                    System.out.println(String.format("---获取topic:%s,分区位置:%s,消息总数:%s",
                            topic,
                            topicPartition.partition(),
                            size
                    ));
                    for(int i = 0;i<size;i++){
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
