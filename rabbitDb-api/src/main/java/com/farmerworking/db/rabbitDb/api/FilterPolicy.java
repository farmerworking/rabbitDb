package com.farmerworking.db.rabbitDb.api;

import java.util.List;

public interface FilterPolicy {
    String createFilter(List<Slice> keys);

    boolean keyMayMatch(Slice key, Slice filter);

    String name();
}
