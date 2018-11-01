package com.farmerworking.db.rabbitDb.impl.writebatch;

import com.farmerworking.db.rabbitDb.api.Slice;

public interface WriteBatchIterateHandler {

    void put(Slice key, Slice value);

    void delete(Slice key);

    void setSequence(long sequence);
}
