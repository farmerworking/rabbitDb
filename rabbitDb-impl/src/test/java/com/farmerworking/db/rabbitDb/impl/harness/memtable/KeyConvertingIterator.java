package com.farmerworking.db.rabbitDb.impl.harness.memtable;


import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.impl.memtable.MemtableIterator;
import com.farmerworking.db.rabbitDb.impl.memtable.ValueType;

public class KeyConvertingIterator implements DBIterator<String, String> {
    private final MemtableIterator iter;

    public KeyConvertingIterator(MemtableIterator iter) {
        this.iter = iter;
    }

    @Override
    public Status getStatus() {
        return iter.getStatus();
    }

    @Override
    public boolean isValid() {
        return iter.isValid();
    }

    @Override
    public void next() {
        iter.next();
    }

    @Override
    public void prev() {
        iter.prev();
    }

    @Override
    public void seekToFirst() {
        iter.seekToFirst();
    }

    @Override
    public void seekToLast() {
        iter.seekToLast();
    }

    @Override
    public void seek(String key) {
        InternalKey seekKey = new InternalKey(key, Long.MAX_VALUE, ValueType.VALUE);
        iter.seek(seekKey);
    }

    @Override
    public String key() {
        assert(isValid());
        return iter.key().getUserKey();
    }

    @Override
    public String value() {
        return iter.value();
    }
}
