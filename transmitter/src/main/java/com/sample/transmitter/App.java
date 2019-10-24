package com.sample.transmitter;

import java.util.List;

public class App {
    public static void main(String[] args) {
        Source source = new KafkaSource();
        Sink sink = new KafkaSink();

        while(!Thread.interrupted()){
            List<byte[]> records = source.get();
            sink.put(records);
        }
    }
}
