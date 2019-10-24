package com.sample.transmitter;

import java.util.*;

public class App {
    public static void main(String[] args) throws Exception {
        String consumerBootstrapServers = args[0];
        String consumerTopicPattern = args[1];

        String producerBootstrapServers = args[2];
        String producerTopicPattern = args[3];

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
