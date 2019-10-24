package com.sample.transmitter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaSource implements Source {

    private final String DEFAULT_BOOTSTRAP_SERVERS = "45.12.236.20:9093,45.12.236.21:9093,45.12.236.34:9093";
    private final String DEFAULT_TOPIC = "input";

    private KafkaConsumer<byte[], byte[]> consumer;

    public KafkaSource(String bootstrapServers, String topicPattern, String groupIdPostfix) throws IOException {
        if (bootstrapServers == null || topicPattern == null || groupIdPostfix == null)
            throw new IllegalArgumentException(this.getClass().getName() + " You must specify arguments for app! see sourcecode!");
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        props.put("client.id", UUID.randomUUID().toString());
        props.put("group.id", "team7developer" + groupIdPostfix);
        props.put("enable.auto.commit", "true");
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
