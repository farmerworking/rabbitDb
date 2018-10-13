package com.farmerworking.db.rabbitDb.writebatch;

import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.memtable.Memtable;
import com.farmerworking.db.rabbitDb.memtable.ValueType;

public class WriteBatchMemtableIterateHandler implements WriteBatchIterateHandler {

    private final Memtable memtable;
    private long sequence;

    public WriteBatchMemtableIterateHandler(Memtable memtable) {
        this.memtable = memtable;
    }

    @Override
    public void put(Slice key, Slice value) {
        memtable.add(new InternalKey(key, sequence, ValueType.VALUE), value);
        sequence++;
    }

    @Override
    public void delete(Slice key) {
        memtable.add(new InternalKey(key, sequence, ValueType.DELETE), Slice.EMPTY_SLICE);
        sequence++;
    }

    @Override
    public void setSequence(long sequence) {
        this.sequence = sequence;
    }
}
