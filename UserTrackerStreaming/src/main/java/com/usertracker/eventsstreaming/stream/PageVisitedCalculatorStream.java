package com.usertracker.eventsstreaming.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

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

        KTable<String, Long> pageViewCounts = pageViewStream
                                                //map val from "User user_1 visited product-1" to "product-1"
                                                .mapValues(value -> {
                                                        String[] parts = value.split(" ");
                                                        return parts[parts.length - 1];
                                                        })
                                                .selectKey((key, val) -> val)  // Select new key (product-1), discard old key
                                                .groupByKey() // Group by new key before aggregation
                                                .count(Materialized.as("counts"));  // Materialize the count store



        // Output the aggregated counts to a new Kafka topic ('number-of-visited-per-page')
        pageViewCounts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();


        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
