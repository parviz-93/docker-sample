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

        HashMap<byte[], String> set = new HashMap<>();
        int doubicate = 0;
        int hash;

        while (!Thread.interrupted()) {
            List<byte[]> records = source.get();
            System.out.println(records.size());

            for (byte[] record : records) {
                hash = Arrays.hashCode(record);
                if (set.get(record) == null) {
                    set.put(record, "");
                } else {
                    doubicate++;
                }
            }

            System.out.println("dublicates: " + doubicate);
            System.out.println("origins   : " + set.size());

//            sink.put(records);
        }

        System.out.println("original: " + set.size());
    }
}
