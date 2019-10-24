package com.sample;

import org.redisson.Redisson;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedisUtil {

    //private static String connect = "0.0.0.0:6379";
    private static String connect = "redis-challenge8team7.apps.ocp.sbercloud.ru:6379";

    public static RedissonClient createClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + connect);
        return Redisson.create(config);
    }

    public static void main(String[] args) {
        final RedissonClient client = createClient();
        final RList<Object> list = client.getList("list");
        list.add("String1");
        list.add("String2");

        list.forEach(System.out::println);

    }
}
