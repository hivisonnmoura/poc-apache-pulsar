package com.example.poc.pulsar.service;

import com.example.poc.pulsar.model.Message;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.spring.PulsarTopicNameResolver;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static com.example.poc.pulsar.util.MessageUtils.parseMessageToByteArray;


@Service
public class PulsarTopicProducer {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final ReactivePulsarClient reactivePulsarClient;
    private final ReactiveProducerCache producerCache;
    private final PulsarTopicNameResolver pulsarTopicNameResolver;
    private final PulsarTopicConsumer pulsarTopicConsumer;

    public PulsarTopicProducer(
            ReactivePulsarClient reactivePulsarClient,
            ReactiveProducerCache producerCache,
            PulsarTopicNameResolver pulsarTopicNameResolver, PulsarTopicConsumer pulsarTopicConsumer) {
        this.reactivePulsarClient = reactivePulsarClient;
        this.producerCache = producerCache;
        this.pulsarTopicNameResolver = pulsarTopicNameResolver;
        this.pulsarTopicConsumer = pulsarTopicConsumer;
    }


    public Mono<MessageId> sendToTopic(String topicName, Mono<Message> messageMono) {
      return getMessageSender(topicName).sendMessage(
              messageMono.transform(parseMessageToByteArray)
                      .flatMap(messageAsByteArray -> {
                          LOGGER.info("About to send a Message to topic:{}", topicName);
                          return Mono.just(MessageSpec.builder(messageAsByteArray).build());
                      })
      ).doOnNext(ign -> {
          LOGGER.info("Calling consumer");
          pulsarTopicConsumer.consumeMessages(topicName);
      }).log();

    }

    private ReactiveMessageSender<byte[]> getMessageSender(String topicName) {
      return reactivePulsarClient
                .messageSender(Schema.BYTES)
                .topic(pulsarTopicNameResolver.resolveTopicName(topicName))
                .maxInflight(100)
                .cache(producerCache)
                .build();
    }
}
