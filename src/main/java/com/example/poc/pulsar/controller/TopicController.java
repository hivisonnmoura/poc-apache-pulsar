package com.example.poc.pulsar.controller;


import com.example.poc.pulsar.model.Message;
import com.example.poc.pulsar.service.PulsarTopicProducer;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class TopicController {

    private final PulsarTopicProducer pulsarTopicProducer;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public TopicController(PulsarTopicProducer pulsarTopicProducer) {
        this.pulsarTopicProducer = pulsarTopicProducer;
    }

    @PostMapping("/topic/send")
    public Mono<MessageId> sendMessageToTopic(@RequestBody Message message) {
        logger.info("Sending Message:{}, to topic", message);
        final var topicName = "TOPIC_NORMAL_"+message.appId();
        return pulsarTopicProducer.sendToTopic(topicName, Mono.just(message));
    }
}
