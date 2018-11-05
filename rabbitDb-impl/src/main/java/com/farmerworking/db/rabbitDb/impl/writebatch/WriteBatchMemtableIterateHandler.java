package com.farmerworking.db.rabbitDb.impl.writebatch;

import com.farmerworking.db.rabbitDb.impl.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.impl.memtable.Memtable;
import com.farmerworking.db.rabbitDb.impl.memtable.ValueType;

public class WriteBatchMemtableIterateHandler implements WriteBatchIterateHandler {

    private final Memtable memtable;
    private long sequence;

    public WriteBatchMemtableIterateHandler(Memtable memtable) {
        this.memtable = memtable;
    }

    @Override
    public void put(String key, String value) {
        memtable.add(new InternalKey(key, sequence, ValueType.VALUE), value);
        sequence++;
    }

    @Override
    public void delete(String key) {
        memtable.add(new InternalKey(key, sequence, ValueType.DELETE), "");
        sequence++;
    }

    @Override
    public void setSequence(long sequence) {
        this.sequence = sequence;
    }
}
