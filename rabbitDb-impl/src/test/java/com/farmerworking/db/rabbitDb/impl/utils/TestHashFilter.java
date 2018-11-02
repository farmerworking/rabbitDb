package com.farmerworking.db.rabbitDb.impl.utils;

import com.farmerworking.db.rabbitDb.api.FilterPolicy;
import com.farmerworking.db.rabbitDb.api.Slice;

import java.util.List;

public class TestHashFilter implements FilterPolicy {
    @Override
    public String createFilter(List<Slice> keys) {
        StringBuilder stringBuilder = new StringBuilder();

        for(Slice s : keys) {
            Coding.putFixed32(stringBuilder, Hash.hash(s.getData(), 1));
        }

        return stringBuilder.toString();
    }

    @Override
    public boolean keyMayMatch(Slice key, Slice filter) {
        int hash = Hash.hash(key.getData(), 1);
        char[] data = filter.getData();
        for (int i = 0; i + 4 <= filter.getSize(); i+=4) {
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
