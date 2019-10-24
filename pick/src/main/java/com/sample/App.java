package com.sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class App {

    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {

        logger.debug("sending data to kafka");
        runProducer();

        final RedissonClient client = RedisUtil.createClient();
        logger.debug("created kafka client with config : {}", client.getConfig().toJSON());
        final RSet<Object> data = client.getSet("dataSet");

        logger.debug("consume data from kafa");
        rumConsumer(data);

        logger.debug("Read all from Redis");
        getAllFromRedis(data);

    }

    private static void rumConsumer(RSet<Object> data) {
        try (final Consumer<Long, String> consumer = KafkaUtil.createConsumer()) {
            int messageCount = 0;
            while (messageCount < KafkaUtil.MESSAGE_COUNT) {
                try {
                    ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
                    if (consumerRecords.count() == 0) {
                        continue;
                    }

                    //print each record.
                    List<String> list = new ArrayList<>(consumerRecords.count());
                    consumerRecords.forEach(record -> {
                        logger.debug("Record Key {}", record.key());
                        logger.debug("Record value {}", record.value());
                        logger.debug("Record partition {}", record.partition());
                        logger.debug("Record offset {}", record.offset());
                        list.add(record.value());
                    });
                    // commits the offset of record to broker.
                    data.addAll(list);
                    consumer.commitAsync();
                    messageCount += consumerRecords.count();
                } catch (Exception e) {
                    logger.error("can not fetch from kafka and add to redis", e);
                }
            }
        }
    }

    private static void getAllFromRedis(RSet<Object> data) {
        for (Object datum : data) {
            logger.debug(datum.toString());
        }
    }

    private static void runProducer() {
        try (final Producer<Long, String> producer = KafkaUtil.createProducer()) {

            for (int i = 0; i < KafkaUtil.MESSAGE_COUNT; i++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(KafkaUtil.TOPIC_FROM, String.format("Message : %s", i));
                producer.send(record);
            }
            producer.flush();
        }
    }


}
