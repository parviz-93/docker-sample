package com.sample.transmitter;

import com.google.common.hash.Hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;

public class MurDuplicateChecker {

    Set<String> set = new HashSet<>();

    public MurDuplicateChecker() {
    }

    public boolean isDuplicated(byte[] record){
        String hash = Hashing.murmur3_128().hashBytes(record).toString();
        if(set.contains(hash)){
            return true;
        } else {
            set.add(hash);
        }
        return false;
    }
}
