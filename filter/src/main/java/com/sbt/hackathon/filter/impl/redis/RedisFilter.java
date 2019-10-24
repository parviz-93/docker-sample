package com.sbt.hackathon.filter.impl.redis;

import com.google.common.hash.Hashing;
import com.sbt.hackathon.filter.Filter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RSet;

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

    @Override
    public void reset() {
        set.clear();
    }

    private byte[] getHashCode(byte[] value) {
        return Hashing.murmur3_128().hashBytes(value).asBytes();
    }

}
