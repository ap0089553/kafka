package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerDemo {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        String bootStrapServer = "127.0.0.1:9092";
        //Producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "first-topic";
            String key = "id-" + i;
            String value = "Hello from the planet " + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (rmd, e) -> {
                if (Objects.isNull(e)) {
                    logger.info("Produced Message. \n" +
                            " Topic: " + rmd.topic() +
                            " Partition: " + rmd.partition() +
                            " Offset: " + rmd.offset() +
                            " Timestamp: " + rmd.timestamp());
                } else {
                    logger.error("Error producing message ", e);
                }
            });
        }
        producer.close();
    }
}
