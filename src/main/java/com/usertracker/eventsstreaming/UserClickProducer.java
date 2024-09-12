package com.usertracker.eventsstreaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class UserClickProducer {

    private static final String[] PAGES = {"home", "about", "products", "contact"};
    private static final String[] USERS = {"user_1", "user_2", "user_3", "user_4"};

    public static void main(String[] args) {
        // Kafka Producer Configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Generate and send random page view events
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            String userId = USERS[random.nextInt(USERS.length)];
            String pageId = PAGES[random.nextInt(PAGES.length)];
            String event = String.format("User %s visited %s", userId, pageId);

            // Create a ProducerRecord with the 'user_clicks' topic, user ID as key, and event as value
            ProducerRecord<String, String> record = new ProducerRecord<>("user_clicks", userId, event);

            // Send the event
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Successfully sent event: %s to partition %d, offset %d%n",
                                      event, metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });

            // Simulate a slight delay between messages
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Close the producer
        producer.close();
    }
}
