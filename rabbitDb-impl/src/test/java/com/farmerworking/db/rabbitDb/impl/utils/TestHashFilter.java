package com.farmerworking.db.rabbitDb.impl.utils;

import com.farmerworking.db.rabbitDb.api.FilterPolicy;

import java.util.List;

public class TestHashFilter implements FilterPolicy {
    @Override
    public String createFilter(List<String> keys) {
        StringBuilder stringBuilder = new StringBuilder();

        for(String s : keys) {
            Coding.putFixed32(stringBuilder, Hash.hash(s.toCharArray(), 1));
        }

        return stringBuilder.toString();
    }

    @Override
    public boolean keyMayMatch(String key, String filter) {
        int hash = Hash.hash(key.toCharArray(), 1);
        char[] data = filter.toCharArray();
        for (int i = 0; i + 4 <= filter.length(); i+=4) {
            if (hash == Coding.decodeFixed32(data, i).getRight()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String name() {
        return "TestHashFilter";
    }
}
