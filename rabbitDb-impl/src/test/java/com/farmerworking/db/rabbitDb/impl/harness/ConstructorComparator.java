package com.farmerworking.db.rabbitDb.impl.harness;

import com.farmerworking.db.rabbitDb.api.DBComparator;

import java.util.Comparator;

public class ConstructorComparator implements Comparator<String> {
    private final DBComparator comparator;

    public ConstructorComparator(DBComparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public int compare(String o1, String o2) {
        return comparator.compare(o1.getBytes(), o2.getBytes());
    }
}
