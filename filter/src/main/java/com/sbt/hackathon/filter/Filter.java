package com.sbt.hackathon.filter;

import java.util.List;

public interface Filter {

    List<byte[]> filtrate(List<byte[]> input);

    void reset();

}
