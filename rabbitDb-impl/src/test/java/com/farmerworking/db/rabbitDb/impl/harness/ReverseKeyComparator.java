package com.farmerworking.db.rabbitDb.impl.harness;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;

class ReverseKeyComparator implements DBComparator {
    private String reverse(char[] bytes) {
        return new StringBuilder(new String(bytes)).reverse().toString();
    }

    private String reverse(String s) {
        return new StringBuilder(s).reverse().toString();
    }

    @Override
    public String name() {
        return "leveldb.ReverseBytewiseComparator";
    }

    @Override
    public char[] findShortestSeparator(char[] start, char[] limit) {
        String s = reverse(start);
        String l = reverse(limit);
        char[] bytes = ByteWiseComparator.getInstance().findShortestSeparator(s.toCharArray(), l.toCharArray());
        return reverse(bytes).toCharArray();
    }

    @Override
    public String findShortestSeparator(String start, String limit) {
        return new String(findShortestSeparator(start.toCharArray(), limit.toCharArray()));
    }

    @Override
    public char[] findShortSuccessor(char[] key) {
        String reverseKey = reverse(key);
        char[] bytes = ByteWiseComparator.getInstance().findShortSuccessor(reverseKey.toCharArray());
        return reverse(bytes).toCharArray();
    }

    @Override
    public String findShortSuccessor(String key) {
        return new String(findShortSuccessor(key.toCharArray()));
    }

    @Override
    public int compare(String o1, String o2) {
        return ByteWiseComparator.getInstance().compare(reverse(o1), reverse(o2));
    }

    @Override
    public int compare(char[] o1, char[] o2) {
        return ByteWiseComparator.getInstance().compare(reverse(o1), reverse(o2));
    }
}
