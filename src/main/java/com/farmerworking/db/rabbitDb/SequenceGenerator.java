package com.farmerworking.db.rabbitDb;

import java.util.concurrent.atomic.AtomicLong;

public class SequenceGenerator {

    private static AtomicLong sequence = new AtomicLong(0);

    public static long last() {
        return sequence.get();
    }

    public static Long generate(int count) {
        long result = sequence.addAndGet(count);
        return result + 1 - count;
    }
}
