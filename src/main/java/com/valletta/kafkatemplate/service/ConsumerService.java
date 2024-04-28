package com.valletta.kafkatemplate.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerService {

//    @KafkaListener(topics = "vallettaTopic", groupId = "foo")
    public void consumer(String message) {
//        log.debug(String.format("Subscribed: %s", message));
        log.info(String.format("Subscribed: %s", message));
    }
}
