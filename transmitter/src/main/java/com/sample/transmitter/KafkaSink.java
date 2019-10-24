package com.sample.transmitter;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;

public class KafkaSink implements Sink{

    private KafkaProducer<byte[],byte[]> producer;
    void init(){

    }


    public void put(List<byte[]> records) {

    }
}
