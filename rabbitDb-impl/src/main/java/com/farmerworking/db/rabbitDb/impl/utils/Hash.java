package com.farmerworking.db.rabbitDb.impl.utils;

public class Hash {
    public static int hash(char[] data, int seed) {
        // Similar to murmur hash
        int m = 0xc6a4a793;
        int r = 24;
        int h = seed ^ (data.length * m);

        // Pick up four bytes at a time
        int offset = 0;
        while (offset + 4 <= data.length) {
            int w = Coding.decodeFixed32(data, offset).getRight();
            offset += 4;
            h += w;
            h *= m;
            h ^= (h >>> 16);
        }

        // Pick up remaining bytes
        switch (data.length - offset) {
            case 3:
                h += ((int)data[offset + 2]) << 16;
            case 2:
                h += ((int)data[offset + 1]) << 8;
            case 1:
                h += (int)data[offset];
                h *= m;
                h ^= (h >>> r);
                break;
            default:
                break;
        }
        return h;
    }

    public static int bloomHash(String key) {
        return hash(key.toCharArray(), 0xbc9f1d34);
    }
}
