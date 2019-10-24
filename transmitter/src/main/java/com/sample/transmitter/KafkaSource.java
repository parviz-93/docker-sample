package com.sample.transmitter;

import javafx.util.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaSource implements Source {

    private final String DEFAULT_BOOTSTRAP_SERVERS = "45.12.236.20:9093,45.12.236.21:9093,45.12.236.34:9093";
    private final String DEFAULT_TOPIC = "team7-test-input";

    private KafkaConsumer<byte[], byte[]> consumer;

    public KafkaSource() {
        Properties props = new Properties();
        props.put("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        props.put("group.id", "team7developer");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", ByteArrayDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "team7developer.jks");
        props.put("ssl.truststore.password", "RyFoP2T4RvXR");
        props.put("ssl.keystore.location", "team7developer.jks");
        props.put("ssl.keystore.password", "RyFoP2T4RvXR");
        props.put("ssl.key.password", "RyFoP2T4RvXR");
        consumer = new KafkaConsumer<byte[], byte[]>(props);
        consumer.subscribe(Pattern.compile(".*-test-input"));
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
