package com.farmerworking.db.rabbitDb.impl.memtable;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Slice;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.skiplist.SkipListIterator;

public class MemtableIterator implements DBIterator<InternalKey, Slice> {

    private final SkipListIterator<InternalEntry> iterator;
    private Status status;

    MemtableIterator(SkipListIterator<InternalEntry> iterator) {
        this.iterator = iterator;
        this.status = Status.ok();
    }

    @Override
    public Status getStatus() {
        return this.status;
    }

    public boolean isValid() {
        return iterator.isValid();
    }

    public void seek(InternalKey key) {
        iterator.seek(new InternalEntry(key, null));
    }

    public void next() {
        iterator.next();
    }

    public void prev() {
        iterator.prev();
    }

    public void seekToFirst() {
        iterator.seekToFirst();
    }

    public void seekToLast() {
        iterator.seekToLast();
    }

    public InternalKey key() {
        return iterator.key().getInternalKey();
    }

    public Slice value() {
        return iterator.key().getValue();
    }
}
