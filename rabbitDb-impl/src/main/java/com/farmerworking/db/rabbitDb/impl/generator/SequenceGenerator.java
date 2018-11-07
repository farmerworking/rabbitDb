package com.farmerworking.db.rabbitDb.impl.generator;

public class SequenceGenerator {
    private static String identifier = "sequence";

    private SequenceGenerator() {
    }

    public static long last() {
        return Generator.last(identifier);
    }

    public static Long generate(int count) {
        return Generator.generate(identifier, count);
    }
}
