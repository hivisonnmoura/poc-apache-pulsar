package com.example.poc.pulsar.util;

import com.example.poc.pulsar.model.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.function.Function;

public interface MessageUtils {

    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    Function<Mono<Message>, Mono<byte[]>> parseMessageToByteArray =
            message -> {
                try {
                  return Mono.just(OBJECT_MAPPER.writeValueAsBytes(message));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    throw Exceptions.propagate(e);
                }
            };

    Function<Flux<byte[]>, Flux<Message>> parseByteArrayToMessage =
            message -> message.flatMap(it -> {
                try {
                    return Mono.just(OBJECT_MAPPER.readValue(it, Message.class));
                } catch (IOException e) {
                    e.printStackTrace();
                    throw Exceptions.propagate(e);
                }
            });

    Function<Flux<org.apache.pulsar.client.api.Message<byte[]>>, Flux<MessageResult<Message>>> messageHandler =
            messageFlux -> messageFlux.flatMap(it ->
                     {
                         Mono<MessageResult<Message>> result = Mono.empty();
                         try {
                             System.out.println(OBJECT_MAPPER.readValue(it.getValue(), Message.class));

                         } catch (IOException e) {
                             e.printStackTrace();
                             throw Exceptions.propagate(e);
                         }
                         return result;
                     }

             );
}
