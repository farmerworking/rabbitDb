package com.farmerworking.db.rabbitDb;

public class ErrorIterator<K, V> implements DBIterator<K, V> {
    private final Status status;

    public ErrorIterator(Status status) {
        this.status = status;
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
    }

    @Override
    public void prev() {
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
        return null;
    }

    @Override
    public V value() {
        return null;
    }
}
