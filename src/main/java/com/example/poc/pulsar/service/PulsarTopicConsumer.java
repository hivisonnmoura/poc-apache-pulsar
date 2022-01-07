package com.example.poc.pulsar.service;


import com.github.lhotari.reactive.pulsar.adapter.ConsumerConfigurer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.spring.PulsarTopicNameResolver;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

import static com.example.poc.pulsar.util.MessageUtils.*;

@Service
public class PulsarTopicConsumer {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final ReactivePulsarClient reactivePulsarClient;
    private final PulsarTopicNameResolver pulsarTopicNameResolver;


    public PulsarTopicConsumer(ReactivePulsarClient reactivePulsarClient, PulsarTopicNameResolver pulsarTopicNameResolver) {
        this.reactivePulsarClient = reactivePulsarClient;
        this.pulsarTopicNameResolver = pulsarTopicNameResolver;

    }

    public void consumeMessages(String topicName){
      createListener(topicName)
               .consumeMessages(banana -> banana.transform(messageHandler))
              .doOnNext(it -> LOGGER.info("Consumed Message:{}", it)

              ).subscribe();
    }

    private ReactiveMessageConsumer<byte[]> createListener(String topicName){
       return reactivePulsarClient
                .messageConsumer(Schema.BYTES)
                .topic(pulsarTopicNameResolver.resolveTopicName(topicName))
                .build();
    }
}
