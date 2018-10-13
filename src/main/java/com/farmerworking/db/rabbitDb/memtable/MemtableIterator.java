package com.farmerworking.db.rabbitDb.memtable;

import com.farmerworking.db.rabbitDb.DBIterator;
import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;
import com.farmerworking.db.rabbitDb.skiplist.SkipListIterator;

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
