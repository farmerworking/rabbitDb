package com.farmerworking.db.rabbitDb.sstable;

import com.farmerworking.db.rabbitDb.Slice;

import java.util.List;

public interface FilterPolicy {
    String createFilter(List<Slice> keys);

    boolean keyMayMatch(Slice key, Slice filter);

    String name();
}
