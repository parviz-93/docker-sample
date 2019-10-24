package com.sample.transmitter;

import java.util.*;

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

        DuplicateChecker duplicateChecker = new DuplicateChecker();
        long doubicate = 0;
        long allrecords =0;

        while (!Thread.interrupted()) {
            List<byte[]> records = source.get();
            System.out.println(records.size());

            for (byte[] record : records) {

                if (duplicateChecker.isDuplicated(record)) {
                    doubicate++;
                }
            }
            allrecords=allrecords+records.size();

            System.out.println("dublicates: " + doubicate);
            System.out.println("allrecords   : " + allrecords);


        }

       // System.out.println("original: " + set.size());
    }
}
