package com.farmerworking.db.rabbitDb.impl;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Status;

public class EmptyIterator<K, V> implements DBIterator<K, V> {
    private Status status;

    public EmptyIterator() {
        this.status = Status.ok();
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public void next() {
        assert false;
    }

    @Override
    public void prev() {
        assert false;
    }

    @Override
    public void seekToFirst() {
    }

    @Override
    public void seekToLast() {
    }

    @Override
    public void seek(K key) {
    }

    @Override
    public K key() {
        assert false;
        return null;
    }

    @Override
    public V value() {
        assert false;
        return null;
    }
}
