package com.usertracker.eventsstreaming.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class UserVisitedPerPageStream {

    public static void main(String[] args) {
        // Kafka Streams Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user_visited_per_page-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Stream from the 'user_clicks' topic
        KStream<String, String> pageViewStream = builder.stream("user_clicks");

        // Transform the value to extract page information
        KStream<String, String> userPageStream = pageViewStream.mapValues(value -> {
            // Extract page information from the value (e.g., "User user_3 visited home")
            String[] parts = value.split(" ");
            return parts[3];  // Extracts the page, assuming the format is always the same
        });

        // Group by key and value (e.g., user ID and page) and count occurrences
        KTable<String, Long> pageViewCounts = userPageStream
            .groupBy((key, page) -> key + "-" + page) // e.g., "user_3-home"
            .count(Materialized.as("user_visited_page_counts-storage"));

        // Output the aggregated counts to a new Kafka topic ('page_view_counts')
        pageViewCounts.toStream().to("user_visited_page_counts", Produced.with(Serdes.String(), Serdes.Long()));

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
