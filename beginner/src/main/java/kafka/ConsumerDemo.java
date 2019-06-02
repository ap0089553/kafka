package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        new ConsumerDemo().run();
    }

    private void run() {
        String bootStrapServer = "localhost:9092";
        String groupId = "my-first-application";
        String topic = "first_topic";
        //Latch for dealing with multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootStrapServer, groupId, topic, countDownLatch);

        //Start the thread
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                logger.error("Error awaiting latch", e);
            }

        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        ConsumerRunnable(String bootStrapServer, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Create Consumer
            this.consumer = new KafkaConsumer<>(properties);

            //Subscribe consumer to a topic
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                //Poll for new data
                //noinspection InfiniteLoopStatement
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(r -> logger.info("Key: " + r.key() + ", Value: " + r.value() + ", Partition: " + r.partition() + ", Offset: " + r.offset()));
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        private void shutDown() {
            consumer.wakeup();
        }
    }

}
