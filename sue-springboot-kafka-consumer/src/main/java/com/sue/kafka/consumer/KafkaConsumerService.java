package com.sue.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * @author sue
 * @date 2020/8/18 9:40
 */

@Component
@Slf4j
public class KafkaConsumerService {
    @KafkaListener(groupId = "group02",topics = "topic02")
    public void onMessage(
            ConsumerRecord<String,Object>record,
            Acknowledgment acknowledgment,
            Consumer<?,?> consumer){
        log.info("消费端接收消息:{}",record.value());
        //手工签收
        acknowledgment.acknowledge();
    }
}
