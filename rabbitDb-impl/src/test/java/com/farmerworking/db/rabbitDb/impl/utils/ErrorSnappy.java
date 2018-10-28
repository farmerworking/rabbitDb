package com.farmerworking.db.rabbitDb.impl.utils;

public class ErrorSnappy extends SnappyWrapper {
    @Override
    public byte[] compress(String s) {
        throw new RuntimeException("compress error");
    }

    @Override
    public String uncompress(byte[] data) {
        throw new RuntimeException("uncompress error");
    }
}
