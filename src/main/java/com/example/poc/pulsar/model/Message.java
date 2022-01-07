package com.example.poc.pulsar.model;


public record Message(
        String appId,
        String content
) {

}
