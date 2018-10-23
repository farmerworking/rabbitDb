package com.farmerworking.db.rabbitDb.impl;

import com.google.common.primitives.SignedBytes;
import org.iq80.leveldb.DBComparator;

import java.util.Arrays;

public class ByteWiseComparator implements DBComparator {

    private static ByteWiseComparator instance;

    private ByteWiseComparator() {
    }

    public synchronized static ByteWiseComparator getInstance() {
        if (instance == null) {
            instance = new ByteWiseComparator();
        }

        return instance;
    }

    public String name() {
        return "leveldb.BytewiseComparator";
    }

    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
        int minLength = Math.min(start.length, limit.length);

        int index = 0;
        while (index < minLength && start[index] == limit[index]) {
            index++;
        }

        if (index == minLength || Byte.MAX_VALUE == start[index] || start[index] + 1 >= limit[index]) {
            return start;
        } else {
            byte[] result = Arrays.copyOf(start, index + 1);
            result[index] = (byte) (start[index] + 1);
            assert compare(result, limit) < 0;
            return result;
        }
    }

    public byte[] findShortSuccessor(byte[] key) {
        int commonLength = 0;
        Byte lastByte = null;

        for (byte b : key) {
            if (Byte.MAX_VALUE != b) {
                lastByte = (byte) (b + 1);
                break;
            } else {
                commonLength++;
            }
        }

        if (lastByte == null) {
            return key;
        } else if (commonLength == 0) {
            return new byte[]{lastByte};
        } else {
            byte[] result = Arrays.copyOf(key, commonLength + 1);
            result[commonLength] = lastByte;
            return result;
        }
    }

    public int compare(byte[] o1, byte[] o2) {
        return SignedBytes.lexicographicalComparator().compare(o1, o2);
    }
}
