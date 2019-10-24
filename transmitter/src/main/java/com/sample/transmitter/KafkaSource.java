package com.sample.transmitter;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaSource implements Source {

    private final String DEFAULT_BOOTSTRAP_SERVERS = "45.12.236.20:9093,45.12.236.21:9093,45.12.236.34:9093";
    private final String DEFAULT_TOPIC = "input";

    private KafkaConsumer<byte[], byte[]> consumer;
    private List<TopicPartition> assignedPartitions = new LinkedList<>();
    private Map<TopicPartition, OffsetAndMetadata> commitedOffsets = new HashMap<>();
    private Map<TopicPartition, OffsetAndMetadata> willBeCommited = new HashMap<>();

    public KafkaSource(String bootstrapServers, String topicPattern, String groupIdPostfix) throws IOException {
        if (bootstrapServers == null || topicPattern == null || groupIdPostfix == null)
            throw new IllegalArgumentException(this.getClass().getName() + " You must specify arguments for app! see sourcecode!");
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        props.put("client.id", UUID.randomUUID().toString());
        props.put("group.id", "team7developer" + groupIdPostfix);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", ByteArrayDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "/app/resources/team7developer.jks");
        props.put("ssl.truststore.password", "RyFoP2T4RvXR");
        props.put("ssl.keystore.location", "/app/resources/team7developer.jks");
        props.put("ssl.keystore.password", "RyFoP2T4RvXR");
        props.put("ssl.key.password", "RyFoP2T4RvXR");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50000");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "50000000");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Pattern.compile(topicPattern), new MyConsumerRebalanceListener());
    }

    public List<byte[]> get() {
        List<byte[]> recordsBytes = new LinkedList();

        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
            records.iterator().forEachRemaining(
                    (record) -> {
                        willBeCommited.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset())
                        );
                        recordsBytes.add(record.value());
                    }
            );
        } catch (Exception e) {
            seekToLastCommited();
        }
        return recordsBytes;
    }

    public void seekToLastCommited() {
        assignedPartitions.forEach(partition -> {
            consumer.seek(partition, commitedOffsets.get(partition));
        });

    }

    @Override
    public void commit() {
        consumer.commitSync(willBeCommited);
        commitedOffsets.putAll(willBeCommited);
    }

    class MyConsumerRebalanceListener implements ConsumerRebalanceListener {

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            partitions.forEach(partition -> {
                assignedPartitions.remove(partition);
                willBeCommited.remove(partition);
                commitedOffsets.remove(partition);
            });
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            consumer.assign(partitions);
            assignedPartitions.clear();
            commitedOffsets.clear();
            willBeCommited.clear();
            assignedPartitions.addAll(partitions);
        }
    }


}
