package com.sample.transmitter;

import java.util.List;

public interface Sink {
    void put(List<byte[]> records);
}
