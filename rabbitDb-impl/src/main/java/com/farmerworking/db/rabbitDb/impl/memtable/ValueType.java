package com.farmerworking.db.rabbitDb.impl.memtable;

public enum ValueType {
    DELETE, VALUE;

    @Override
    public String toString() {
        return this == DELETE ? "DELETE" : "VALUE";
    }
}
