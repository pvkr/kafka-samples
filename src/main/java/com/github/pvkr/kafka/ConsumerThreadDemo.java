package com.github.pvkr.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

public class ConsumerThreadDemo {

    private static Logger log = LoggerFactory.getLogger(ConsumerThreadDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-forth-app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread consumer = new ConsumerThread(latch, properties, "first_topic");
        new Thread(consumer).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("something is wrong");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("something is wrong");
        } finally {
            log.info("App is closed");
        }
    }

    private static class ConsumerThread implements Runnable {
        private static Logger log = LoggerFactory.getLogger(ConsumerThread.class);

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch, Properties properties, String topic) {
            this.latch = latch;

            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        log.info("value={}", record.value());
                        log.info("key={}", record.key());
                        log.info("offset={}", record.offset());
                        log.info("partition={}", record.partition());
                    }
                }
            } catch (WakeupException we) {
                log.info("Receive shutdown signal!");
            } catch (Exception ie) {
                log.info("Receive interrupt signal!", ie);
            } finally {
                latch.countDown();
                consumer.close();
            }
        }

        public void shutdown() {
            log.info("Send shutdown signal");
            consumer.wakeup();
        }
    }
}
