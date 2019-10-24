package com.sample.transmitter;

import java.util.List;

public interface Source {
    List<byte[]> get();
    void commit();
}
