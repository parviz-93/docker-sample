package com.sbt.hackathon.filter.impl.redis;

import com.sbt.hackathon.filter.Filter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RSet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor()
public class RedisFilter implements Filter {

    @NonNull
    private RSet<byte[]> set;
    
    public List<byte[]> filtrate(List<byte[]> input) {
        List<byte[]> output = new ArrayList<>();
        for (byte[] value : input) {
            if (set.add(getHashCode(value))) {
                output.add(value);
            }
        }
        return output;
    }

    private byte[] getHashCode(byte[] value) {
        return String.valueOf(new String(value, StandardCharsets.UTF_8).hashCode()).getBytes();
    }
    
}
