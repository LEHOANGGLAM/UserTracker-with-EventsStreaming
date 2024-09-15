# UserTraker Application with Events Streaming
A Events Streaming sample application to learn how to apply events streaming technology with Aphache Kafka

# Getting started with Docker Compose
1. Get the latest source code
2. Open terminal of your choice, go to `UserTracker-with-EventsStreaming` directory, run `docker compose -f docker-compose.yml up`, wait for all the containers up and running
3. Create kafka topics:
```bash
docker exec -it <kafka_container_id> kafka-topics --create --topic user_clicks --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it <kafka_container_id> kafka-topics --create --topic page_visited_counts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it <kafka_container_id> kafka-topics --create --topic user_visited_page_counts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```
4. Open the UserTrackerApi project in your favorite IDEA, set up the Java SDK and launch the project:
4. Open the UserTrackerStreaming project in your favorite IDEA, set up the Java SDK and launch two streaming flows:
5. Open your browser, now you can access the api, sample: `http://localhost:8080/user-id/products/product-id` 

