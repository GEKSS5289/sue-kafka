package com.sue.kafka.producer.test;

import com.sue.kafka.producer.KafkaproducerService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author sue
 * @date 2020/8/18 10:01
 */

@RunWith(SpringRunner.class)
@SpringBootTest
public class Application {
    @Autowired
    private KafkaproducerService kafkaproducerService;
    @Test
    public void send(){
        String topic = "topic02";
        for(int i = 0;i<10;i++){
            kafkaproducerService.sendMessage(topic,"hello"+i);
        }

    }
}
