package com.sample.transmitter;

import java.util.*;

public class App {
    public static void main(String[] args) throws Exception{
        Source source = new KafkaSource(args[0],args[1],args[2]);
        Sink sink = new KafkaSink(args[3],args[4]);

        Set<Integer> set = new HashSet<>();
        int doubicate = 0;
        int hash;

        while(!Thread.interrupted()){
            List<byte[]> records = source.get();
            System.out.println(records.size());

            for (byte[] record : records) {
                hash = Arrays.hashCode(record);
                if(!set.contains(hash)){
                    set.add(hash);
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
