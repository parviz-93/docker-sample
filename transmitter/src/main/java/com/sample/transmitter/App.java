package com.sample.transmitter;

import com.sbt.hackathon.filter.Filter;
import com.sbt.hackathon.filter.impl.redis.RedisFilterFactory;

import java.util.List;
import java.util.UUID;

public class App {
    public static void main(String[] args) throws Exception {

        for (String arg : args) {
            System.out.println(arg);
        }

        String consumerBootstrapServers = System.getenv("kafka-external");
        String consumerTopicPattern = System.getenv("topic-for-consumer");

        String producerBootstrapServers = System.getenv("kafka-producer");
        String producerTopicPattern = System.getenv("topic-for-producer");

        Source source = new KafkaSource(consumerBootstrapServers, consumerTopicPattern, UUID.randomUUID().toString());
        Sink sink = new KafkaSink(producerBootstrapServers, producerTopicPattern);

        RedisFilterFactory filterFactory = new RedisFilterFactory("0.0.0.0:6379");

        Filter filter = filterFactory.getInstance("LOCAL_TEST");
        filter.reset();

        while(!Thread.interrupted()){
            List<byte[]> read = source.get();
            System.out.println("Прочитано " + read.size());
            List<byte[]> write = filter.filtrate(read);
            System.out.println("Записано " + write.size());
            sink.put(write);
            System.out.println("Дубликатов отброшено " + (read.size() - write.size()));
        }

        filterFactory.close();
    }
}
