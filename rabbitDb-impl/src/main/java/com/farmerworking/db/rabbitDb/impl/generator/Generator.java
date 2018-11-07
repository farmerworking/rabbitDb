package com.farmerworking.db.rabbitDb.impl.generator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class Generator {
    private static ConcurrentHashMap<String, AtomicLong> maps = new ConcurrentHashMap<>();

    static long last(String identifier) {
        maps.putIfAbsent(identifier, new AtomicLong(0));

        return maps.get(identifier).get();
    }

    static Long generate(String identifier, int count) {
        maps.putIfAbsent(identifier, new AtomicLong(0));

        long result = maps.get(identifier).addAndGet(count);
        return result + 1 - count;
    }
}
