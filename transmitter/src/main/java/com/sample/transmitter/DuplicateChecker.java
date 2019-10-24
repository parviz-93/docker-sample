package com.sample.transmitter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;

public class DuplicateChecker {
    Set<String> set = new HashSet<>();
    MessageDigest digest = MessageDigest.getInstance( "SHA-1" );

    public DuplicateChecker() throws NoSuchAlgorithmException {
    }

    public boolean isDuplicated(byte[] record){
        String hash = byteArray2Hex(digest.digest(record));
        if(set.contains(hash)){
            return true;
        }else
        {
            set.add(hash);
        }
        return false;
    }

    private static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
