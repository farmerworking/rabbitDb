package com.farmerworking.db.rabbitDb.api;

import java.util.List;

public interface FilterPolicy {
    String createFilter(List<String> keys);

    boolean keyMayMatch(String key, String filter);

    String name();
}
