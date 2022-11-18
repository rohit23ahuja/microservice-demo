package com.microservice.demo.twitter.to.kafka.service.runner.impl;

import java.util.Arrays;

import javax.annotation.PreDestroy;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.microservice.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservice.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservice.demo.twitter.to.kafka.service.runner.StreamRunner;

import lombok.extern.slf4j.Slf4j;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Slf4j
@Component
@ConditionalOnProperty(value = "twitter-to-kafka-service.v2-enabled", havingValue = "false", matchIfMissing = false)
public class TwitterKafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
            TwitterKafkaStatusListener twitterKafkaStatusListener) {
        super();
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords()
                .toArray(new String[0]);
        FilterQuery query = new FilterQuery(keywords);
        twitterStream.filter(query);
        log.info("Started filtering twitter stream for keywords - {}", Arrays.toString(keywords));
    }

}
