package org.playground.kafka.twitter.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearch {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch.class);
    private static JsonParser jsonParser = new JsonParser();

    private static KafkaConsumer<String, String> createConsumer() {
        String bootStrapServer = "localhost:9092";
        String groupId = "my-first-application";
        String topic = "twitter-tweets";

        //Create consumer configs
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Subscribe consumer to a topic
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    private static RestHighLevelClient createClient() {

        String hostName = "###";
        String userName = "###";
        String passWord = "###";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, passWord));
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(restClientBuilder);
    }

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            int recordCount = records.count();
            logger.info("Received " + recordCount + " records");
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord record : records) {
                try {
                    String tweetId = extractIdFromTweet(record.value()); //this is to make consumer idempotent
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", tweetId)
                            .source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("Skipping Bad Data: " +  record.value());
                }
            }
            try {
                //Consume only if records are available
                if (recordCount > 0) {
                    client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    /*IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();
                    logger.info(id);*/
                }
            } catch (ElasticsearchException ese) {
                logger.error("Error consuming tweet", ese);
            }

            logger.info("Committing the batch");
            consumer.commitSync();
            logger.info("Offsets have be committed");

        }
        //Close the client gracefully
        //client.close();
    }

    private static String extractIdFromTweet(Object tweetJson) {
        return jsonParser.parse((String) tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
