package com._4paradigm.benchmark.memory;
import redis.clients.jedis.*;

import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicLong;

public class RedisTest implements Test{
    private JedisPoolConfig poolConfig;
    private JedisCluster cluster;
    static private String endpoints;

    private static String baseKey;
    private long base;
    private AtomicLong deta = new AtomicLong(0);
    private Random random = new Random(System.currentTimeMillis());
    static {
        try {
            Properties prop = new Properties();
            prop.load(FedbTest.class.getClassLoader().getResourceAsStream("benchmark.properties"));
            endpoints = prop.getProperty("redis_addr");
            baseKey = prop.getProperty("base_key");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean init() {
        poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();

        for (String endpoint : endpoints.trim().split(",")) {
            String[] arr = endpoint.split(":");
            if (arr.length != 2) {
                continue;
            }
            System.out.println("add endpoint " + endpoint);
            nodes.add(new HostAndPort(arr[0], Integer.valueOf(arr[1])));
        }
        base = Long.valueOf(baseKey);
        cluster = new JedisCluster(nodes, poolConfig);
        return true;
    }

    @Override
    public void put() {
        while (true) {
            String key = baseKey + (base + deta.getAndIncrement());
            cluster.set(key, key);
        }
    }
}
