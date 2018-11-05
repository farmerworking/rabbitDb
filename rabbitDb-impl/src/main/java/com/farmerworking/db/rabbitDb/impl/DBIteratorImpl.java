package com.farmerworking.db.rabbitDb.impl;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.impl.memtable.MemtableIterator;
import com.farmerworking.db.rabbitDb.impl.memtable.ValueType;


public class DBIteratorImpl implements DBIterator<String, String> {

    private final MemtableIterator memtableIterator;
    private final long sequence;
    private final DBComparator comparator;

    public DBIteratorImpl(MemtableIterator memtableIterator, long sequence,
                          DBComparator comparator) {
        this.memtableIterator = memtableIterator;
        this.sequence = sequence;
        this.comparator = comparator;
    }

    public String key() {
        return memtableIterator.key().getUserKey();
    }

    public String value() {
        return memtableIterator.value();
    }

    public boolean isValid() {
        return memtableIterator.isValid();
    }

    public void seekToFirst() {
        memtableIterator.seekToFirst();
        nextValidUserKey(null);
    }

    public void seek(String key) {
        memtableIterator.seek(new InternalKey(key, sequence, null));
        nextValidUserKey(null);
    }

    public Status getStatus() {
        return memtableIterator.getStatus();
    }

    public void next() {
        nextValidUserKey(memtableIterator.key().getUserKey());
    }

    public void seekToLast() {
        memtableIterator.seekToLast();
        prevValidUserKey();
    }

    public void prev() {
        memtableIterator.prev();
        prevValidUserKey();
    }

    private void prevValidUserKey() {
        InternalKey currentKey = null;
        while (memtableIterator.isValid()) {
            InternalKey internalKey = memtableIterator.key();

            if (internalKey.getSequence() > sequence) {
                // skip new record since this iterator creation
                memtableIterator.prev();
            } else if (currentKey == null ||
                    comparator.compare(currentKey.getUserKey().toCharArray(), internalKey.getUserKey().toCharArray())
                            == 0) {
                currentKey = internalKey;
                memtableIterator.prev();
            } else if (currentKey.getValueType() == ValueType.DELETE) {
                currentKey = null;
            } else {
                break;
            }
        }

        if (currentKey != null && currentKey.getValueType() == ValueType.VALUE) {
            memtableIterator.next();
        }
    }

    private void nextValidUserKey(String deletedKey) {
        while (memtableIterator.isValid()) {
            InternalKey internalKey = memtableIterator.key();

            if (internalKey.getSequence() > sequence) {
                // skip new record since this iterator creation
                memtableIterator.next();
            } else if (internalKey.getValueType() == ValueType.DELETE) {
                deletedKey = internalKey.getUserKey();
                memtableIterator.next();
            } else if (deletedKey != null
                    && comparator.compare(deletedKey, internalKey.getUserKey()) == 0) {
                memtableIterator.next();
            } else {
                break;
            }
        }
    }
}
