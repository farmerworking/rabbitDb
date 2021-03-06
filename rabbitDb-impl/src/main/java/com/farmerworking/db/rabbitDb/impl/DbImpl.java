package com.farmerworking.db.rabbitDb.impl;

import com.farmerworking.db.rabbitDb.api.*;
import com.farmerworking.db.rabbitDb.impl.generator.SequenceGenerator;
import com.farmerworking.db.rabbitDb.impl.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.impl.memtable.Memtable;
import com.farmerworking.db.rabbitDb.impl.writebatch.WriteBatchImpl;
import com.farmerworking.db.rabbitDb.impl.writebatch.WriteBatchMemtableIterateHandler;

public class DbImpl {

    private final Memtable memtable;
    private final Options options;
    private final DBComparator comparator;

    public DbImpl(Options options) {
        this.options = options;
        if (options.comparator() == null) {
            this.comparator = ByteWiseComparator.getInstance();
        } else {
            this.comparator = options.comparator();
        }
        this.memtable = new Memtable(this.comparator);
    }

    public String get(String key) throws DBException {
        return get(key, getDefaultReadOptions());
    }

    public Snapshot put(String key, String value) throws DBException {
        WriteBatchImpl writeBatch = createWriteBatch();
        writeBatch.put(key, value);
        return write(writeBatch);
    }

    public Snapshot delete(String key) throws DBException {
        WriteBatchImpl writeBatch = createWriteBatch();
        writeBatch.delete(key);
        return write(writeBatch);
    }

    public DBIteratorImpl iterator() {
        return iterator(getDefaultReadOptions());
    }

    public DBIteratorImpl iterator(ReadOptions options) {
        long sequence = ((SnapshotImpl) options.snapshot()).getSequence();
        return new DBIteratorImpl(memtable.iterator(), sequence, this.comparator);
    }

    public Snapshot write(WriteBatchImpl writeBatch) throws DBException {
        writeBatch.setSequence(SequenceGenerator.generate(writeBatch.getCount()));

        long memoryUsageAfterUpdate = writeBatch.approximateSize() + memtable.approximateMemoryUsage();
        if (memoryUsageAfterUpdate > this.options.writeBufferSize()) {
            throw new DBException("memory has run out");
        } else {
            writeBatch.iterate(new WriteBatchMemtableIterateHandler(memtable));
            return new SnapshotImpl(writeBatch.getSequence() + writeBatch.getCount() - 1);
        }
    }

    public String get(String key, ReadOptions options) throws DBException {
        long sequence = ((SnapshotImpl) options.snapshot()).getSequence();
        return memtable.get(new InternalKey(key, sequence, null));
    }

    public Snapshot getSnapshot() {
        return new SnapshotImpl(SequenceGenerator.last());
    }

    public WriteBatchImpl createWriteBatch() {
        return new WriteBatchImpl();
    }

    private ReadOptions getDefaultReadOptions() {
        ReadOptions options = new ReadOptions();
        options.snapshot(getSnapshot());
        return options;
    }
}
