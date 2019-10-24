package com.sbt.hackathon.filter.impl.redis;

import org.jetbrains.annotations.NotNull;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public final class RedisFilterFactory {

    private RedissonClient redissonClient;

    public RedisFilterFactory(@NotNull String connect) {
        redissonClient = createClient(connect);
    }

    public RedisFilter getInstance(@NotNull String setName) {
        return new RedisFilter(redissonClient.getSet(setName));
    }

    public void close() {
        redissonClient.shutdown();
    }

    private static RedissonClient createClient(@NotNull String connect) {
        String address = "redis://" + System.getProperty("redis", connect);
        Config config = new Config();
        config.useSingleServer().setAddress(address);
        return Redisson.create(config);
    }

}
