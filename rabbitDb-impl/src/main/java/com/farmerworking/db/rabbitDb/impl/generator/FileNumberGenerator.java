package com.farmerworking.db.rabbitDb.impl.generator;

public class FileNumberGenerator {
    private static String identifier = "fileNumber";

    private FileNumberGenerator() {
    }

    public static long last() {
        return Generator.last(identifier);
    }

    public static Long generate(int count) {
        return Generator.generate(identifier, count);
    }
}
