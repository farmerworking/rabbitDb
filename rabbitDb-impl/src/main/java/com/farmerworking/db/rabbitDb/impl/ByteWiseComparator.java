package com.farmerworking.db.rabbitDb.impl;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.google.common.primitives.Chars;
import com.google.common.primitives.SignedBytes;

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

    public char[] findShortestSeparator(char[] start, char[] limit) {
        int minLength = Math.min(start.length, limit.length);

        int index = 0;
        while (index < minLength && start[index] == limit[index]) {
            index++;
        }

        if (index == minLength || Character.MAX_VALUE == start[index] || start[index] + 1 >= limit[index]) {
            return start;
        } else {
            char[] result = Arrays.copyOf(start, index + 1);
            result[index] = (char) (start[index] + 1);
            assert compare(result, limit) < 0;
            return result;
        }
    }

    @Override
    public String findShortestSeparator(String start, String limit) {
        return new String(findShortestSeparator(start.toCharArray(), limit.toCharArray()));
    }

    public char[] findShortSuccessor(char[] key) {
        int commonLength = 0;
        Character lastByte = null;

        for (char b : key) {
            if (Character.MAX_VALUE != b) {
                lastByte = (char) (b + 1);
                break;
            } else {
                commonLength++;
            }
        }

        if (lastByte == null) {
            return key;
        } else if (commonLength == 0) {
            return new char[]{lastByte};
        } else {
            char[] result = Arrays.copyOf(key, commonLength + 1);
            result[commonLength] = lastByte;
            return result;
        }
    }

    @Override
    public String findShortSuccessor(String key) {
        return new String(findShortSuccessor(key.toCharArray()));
    }

    @Override
    public int compare(String o1, String o2) {
        return compare(o1.toCharArray(), o2.toCharArray());
    }

    public int compare(char[] o1, char[] o2) {
        return Chars.lexicographicalComparator().compare(o1, o2);
    }
}
