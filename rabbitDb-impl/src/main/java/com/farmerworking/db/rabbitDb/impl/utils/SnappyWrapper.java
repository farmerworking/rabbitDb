package com.farmerworking.db.rabbitDb.impl.utils;

import org.iq80.snappy.Snappy;


public class SnappyWrapper {
    public byte[] compress(String s) {
        return Snappy.compress(s.getBytes());
    }

    public String uncompress(byte[] data) {
        return new String(Snappy.uncompress(data, 0, data.length));
    }
}
