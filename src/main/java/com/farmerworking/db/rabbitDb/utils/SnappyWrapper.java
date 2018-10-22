package com.farmerworking.db.rabbitDb.utils;

import org.iq80.snappy.Snappy;


public class SnappyWrapper {

    public static byte[] compress(String s) {
        return Snappy.compress(s.getBytes());
    }

    public static String uncompress(byte[] data) {
        return new String(Snappy.uncompress(data, 0, data.length));
    }
}
