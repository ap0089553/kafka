package org.playground.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {
    private static final Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class);
    private static final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter-tweets");
        KStream<String, String> filteredStream = inputTopic.filter((key, value) -> filterUserFollowersTweet(value) > 10000);
        filteredStream.to("important-tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start the streams application
        kafkaStreams.start();
    }

    private static Integer filterUserFollowersTweet(String jsonTweet) {
        try {
            return jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            logger.error("Error reading tweets");
            return 0;
        }
    }
}