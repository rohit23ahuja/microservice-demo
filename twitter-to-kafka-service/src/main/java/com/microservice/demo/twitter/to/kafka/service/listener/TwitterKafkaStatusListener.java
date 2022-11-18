package com.microservice.demo.twitter.to.kafka.service.listener;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Slf4j
@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    @Override
    public void onStatus(Status status) {
        log.info("Status - [{}]", status.getText());
    }
}
