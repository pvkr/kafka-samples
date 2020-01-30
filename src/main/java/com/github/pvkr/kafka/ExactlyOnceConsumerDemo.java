package com.github.pvkr.kafka;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class ExactlyOnceConsumerDemo {
    private static Logger log = LoggerFactory.getLogger(ExactlyOnceConsumerDemo.class);

    public static void main(String[] args) {
        OffsetStorage offsetStorage = new OffsetStorage("offset-storage.txt");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "exactly-once-example");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("first_topic"), new CustomConsumerRebalanceListener(consumer, offsetStorage));

        handleMessages(consumer, offsetStorage);

    }

    private static void handleMessages(KafkaConsumer<String, String> consumer, OffsetStorage offsetStorage) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                log.info("partition = {}, offset = {}, key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
                // persist offset
                offsetStorage.saveOffset(record.topic(), record.partition(), record.offset());
            });
        }
    }

    private static class CustomConsumerRebalanceListener implements ConsumerRebalanceListener {
        private final KafkaConsumer<String, String> consumer;
        private final OffsetStorage offsetStorage;

        public CustomConsumerRebalanceListener(KafkaConsumer<String, String> consumer, OffsetStorage offsetStorage) {
            this.consumer = consumer;
            this.offsetStorage = offsetStorage;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                offsetStorage.saveOffset(partition.topic(), partition.partition(), consumer.position(partition));
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, offsetStorage.getNextOffset(partition.topic(), partition.partition()));
            }
        }
    }

    private static class OffsetStorage {
        private final String storageName;

        public OffsetStorage(String storageName) {
            this.storageName = storageName;
        }

        @SneakyThrows
        public void saveOffset(String topic, int partition, long offset) {
            Map<String, Long> offsetMap = getOffsetMap();
            offsetMap.put(offsetKey(topic, partition), offset);

            List<String> lines = offsetMap.entrySet().stream().map(entry -> entry.getKey() + " " + entry.getValue()).collect(Collectors.toList());
            Files.write(Paths.get(storageName), lines, CREATE, WRITE);
        }

        @SneakyThrows
        public long getNextOffset(String topic, int partition) {
            return getOffsetMap().getOrDefault(offsetKey(topic, partition), -1L) + 1;
        }

        private Map<String, Long> getOffsetMap() throws IOException {
            return Files.exists(Paths.get(storageName))
                ? Files.lines(Paths.get(storageName)).filter(line -> !line.isEmpty()).collect(Collectors.toMap(
                    line -> line.split(" ")[0],
                    line -> Long.parseLong(line.split(" ")[1])
                    ))
                : new HashMap<>();
        }

        private String offsetKey(String topic, int partition) {
            return topic + "-" + partition;
        }
    }
}
