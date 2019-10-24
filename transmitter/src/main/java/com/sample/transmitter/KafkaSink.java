package com.sample.transmitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.List;
import java.util.Properties;

public class KafkaSink implements Sink {

    private final String DEFAULT_BOOTSTRAP_SERVERS = "45.12.236.20:9093,45.12.236.21:9093,45.12.236.34:9093";
    private final String DEFAULT_TOPIC = "team7-test-output";
    private String topic;

    private KafkaProducer<byte[], byte[]> producer;

    public KafkaSink(String bootstrapServers, String topic) {
        if (bootstrapServers == null || topic == null)
            throw new IllegalArgumentException(this.getClass().getName() + " You must specify arguments for app! see sourcecode!");

        this.topic = topic;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", "/app/resources//team7developer.jks");
        properties.put("ssl.truststore.password", "RyFoP2T4RvXR");
        properties.put("ssl.keystore.location", "/app/resources/team7developer.jks");
        properties.put("ssl.keystore.password", "RyFoP2T4RvXR");
        properties.put("ssl.key.password", "RyFoP2T4RvXR");
        producer = new KafkaProducer<byte[], byte[]>(properties);
    }


    public void put(List<byte[]> records) {
        records.forEach(record -> producer.send(new ProducerRecord<byte[], byte[]>(topic, record)));
    }
}
