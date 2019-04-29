package com.github.pvkr.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerSinglePartitionDemo {

    private static Logger log = LoggerFactory.getLogger(ProducerSinglePartitionDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "key", "Hello World!" + i);

            producer.send(record, (meta, e) -> {
                    log.info("Received new metadata: Topic: {}", meta.topic());
                    log.info("Received new metadata: Partition: {}", meta.partition());
                    log.info("Received new metadata: Offset: {}", meta.offset());
                   log.info("Received new metadata: Timestamp: {}", meta.timestamp());
            });
        }
        producer.flush();
        producer.close();
    }
}
