package com.farmerworking.db.rabbitDb.impl.writebatch;

public interface WriteBatchIterateHandler {

    void put(String key, String value);

    void delete(String key);

    void setSequence(long sequence);
}
