package com.farmerworking.db.rabbitDb.impl.harness;

import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import org.iq80.leveldb.DBComparator;

class ReverseKeyComparator implements DBComparator {
    private String reverse(byte[] bytes) {
        return new StringBuilder(new String(bytes)).reverse().toString();
    }

    @Override
    public String name() {
        return "leveldb.ReverseBytewiseComparator";
    }

    @Override
    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
        String s = reverse(start);
        String l = reverse(limit);
        byte[] bytes = ByteWiseComparator.getInstance().findShortestSeparator(s.getBytes(), l.getBytes());
        return reverse(bytes).getBytes();
    }

    @Override
    public byte[] findShortSuccessor(byte[] key) {
        String reverseKey = reverse(key);
        byte[] bytes = ByteWiseComparator.getInstance().findShortSuccessor(reverseKey.getBytes());
        return reverse(bytes).getBytes();
    }

    @Override
    public int compare(byte[] o1, byte[] o2) {
        return ByteWiseComparator.getInstance().compare(reverse(o1).getBytes(), reverse(o2).getBytes());
    }
}
