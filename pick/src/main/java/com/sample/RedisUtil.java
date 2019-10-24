package com.sample;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedisUtil {

    private static String connect = "0.0.0.0:6379";

    public static RedissonClient createClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + System.getProperty("redis", connect));
        return Redisson.create(config);
    }
}
