package com.sample.transmitter;

import javafx.util.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaSource implements Source {

    private final String DEFAULT_BOOTSTRAP_SERVERS = "45.12.236.20:9093,45.12.236.21:9093,45.12.236.34:9093";
    private final String DEFAULT_TOPIC = "team7-test-input";

    private KafkaConsumer<byte[], byte[]> consumer;

    public KafkaSource(String bootstrapServers, String topicPattern, String groupIdPostfix) throws IOException {
        if(bootstrapServers==null||topicPattern==null||groupIdPostfix==null)
            throw new IllegalArgumentException(this.getClass().getName()+" You must specify arguments for app! see sourcecode!");
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        props.put("client.id", UUID.randomUUID().toString());
        props.put("group.id", "team7developer"+groupIdPostfix);
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", ByteArrayDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "team7developer.jks");
        props.put("ssl.truststore.password", "RyFoP2T4RvXR");
        props.put("ssl.keystore.location", "team7developer.jks");
        props.put("ssl.keystore.password", "RyFoP2T4RvXR");
        props.put("ssl.key.password", "RyFoP2T4RvXR");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50000");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "50000000");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Pattern.compile(topicPattern));
//        consumer.seekToBeginning(consumer.assignment());
    }

    public List<byte[]> get() {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(2000);
        List<byte[]> recordsBytes = new LinkedList();

        records.iterator().forEachRemaining(
                (record) -> {
                    recordsBytes.add(record.value());
                }
        );

        return recordsBytes;
    }

}
