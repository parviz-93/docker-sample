package com.sample.generator;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Generator {

    private final String DEFAULT_BOOTSTRAP_SERVERS = "45.12.236.20:9093,45.12.236.21:9093,45.12.236.34:9093";
    private final String DEFAULT_TOPIC = "team7-test-input";
    private long sended = 0;
    private long doubleCount = 0;

    private Logger logger = LoggerFactory.getLogger(Generator.class);
    private String topic;

    private static KafkaProducer<byte[], byte[]> producer;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Generator generator = new Generator();
        generator.init(null,null);
        generator.start();
    }

    public void init(String bootstrapServers, String topic) {
        this.topic = topic == null ? DEFAULT_TOPIC : topic;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers == null ? DEFAULT_BOOTSTRAP_SERVERS : bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", "team7developer.jks");
        properties.put("ssl.truststore.password", "RyFoP2T4RvXR");
        properties.put("ssl.keystore.location", "team7developer.jks");
        properties.put("ssl.keystore.password", "RyFoP2T4RvXR");
        properties.put("ssl.key.password", "RyFoP2T4RvXR");
        producer = new KafkaProducer<byte[], byte[]>(properties);
    }

    public void start() {
        try {
            while (!Thread.interrupted()) {
                send();
                if (Math.random() > 0.8) {
                    send();
                    doubleCount++;
                }
                if (sended % 1000 == 0) {
                    logger.info("sended: {} double count: {}", sended, doubleCount);
                }
            }
        } finally {
            logger.info("Close producer");
            producer.close();
        }
    }

    private Future<RecordMetadata> send() {
        Future<RecordMetadata> f = producer.send(new ProducerRecord<byte[], byte[]>(topic, generatePayload()));
        sended++;
        return f;
    }

    private byte[] generatePayload() {
        return RandomStringUtils.random(10240, true, true).getBytes(Charset.forName("UTF8"));
    }
}
