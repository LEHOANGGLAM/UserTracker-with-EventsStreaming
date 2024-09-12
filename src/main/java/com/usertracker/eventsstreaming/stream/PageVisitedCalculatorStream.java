package com.usertracker.eventsstreaming.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class PageVisitedCalculatorStream {

    private static final String INPUT_TOPIC = "user_clicks";
    private static final String OUTPUT_TOPIC = "page_visited_counts";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        // Set properties for Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "page-visited-calculator-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Build the stream processing topology
        StreamsBuilder builder = new StreamsBuilder();

        // Stream from the 'user_clicks' topic
        KStream<String, String> pageViewStream = builder.stream(INPUT_TOPIC);

        // Transform the value to extract page information
        KStream<String, String> pageStream = pageViewStream.mapValues(value -> {
            // Extract page information from the value (e.g., "User user_1 visited product-1")
            String[] parts = value.split(" ");
            return parts[parts.length - 1];  // Extracts the page (e.g., "product-1"), assuming the format is always the same
        });

        // Group by page and count occurrences
        KTable<String, Long> pageViewCounts = pageStream
            .groupBy((key, page) -> page)  // Group by page (e.g., "product-1")
            .count(Materialized.as("page_visited_counts-storage"));  // Materialize the count store

        // Output the aggregated counts to a new Kafka topic ('number-of-visited-per-page')
        pageViewCounts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
