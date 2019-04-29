package com.github.pvkr.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World!");

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata meta, Exception e) {
                log.info("Received new metadata: Topic: {}", meta.topic());
                log.info("Received new metadata: Partition: {}", meta.partition());
                log.info("Received new metadata: Offset: {}", meta.offset());
                log.info("Received new metadata: Timestamp: {}", meta.timestamp());
            }
        });
        producer.flush();
        producer.close();
    }
}
