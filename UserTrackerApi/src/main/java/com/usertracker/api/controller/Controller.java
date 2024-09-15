package com.usertracker.api.controller;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {
    private static final String OUTPUT_TOPIC = "user_clicks";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @GetMapping("/{userId}/products/{productId}")
    public ResponseEntity<String> getProductId(@PathVariable String userId, @PathVariable String productId) {
        // Configure the producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Produce the updated count to the output topic
        String event = String.format("User %s visited %s", userId, productId);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(OUTPUT_TOPIC, null, String.valueOf(event));
        producer.send(producerRecord);

        return ResponseEntity.ok(productId + "\nThis is the discription");
    }
}