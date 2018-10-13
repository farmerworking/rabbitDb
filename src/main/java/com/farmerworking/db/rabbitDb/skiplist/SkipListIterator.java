package com.farmerworking.db.rabbitDb.skiplist;

import com.farmerworking.db.rabbitDb.DBIterator;
import com.farmerworking.db.rabbitDb.Status;

public class SkipListIterator<T> implements DBIterator<T, T> {

    private final SkipList<T> skipList;
    private Status status;
    private Node<T> current;

    SkipListIterator(SkipList<T> skipList) {
        this.skipList = skipList;
        this.current = null;
        this.status = Status.ok();
    }

    @Override
    public Status getStatus() {
        return status;
    }

    public boolean isValid() {
        return current != skipList.head && current != skipList.tail && current != null;
    }

    public T key() {
        assert isValid();
        return current.getItem();
    }

    @Override
    public T value() {
        throw new UnsupportedOperationException();
    }

    public void next() {
        assert current != null && current != skipList.tail;
        this.current = current.getLevelNext().get(0);
    }

    public void prev() {
        assert current != null && current != skipList.head;
        this.current = current.getLevel0PrevNode();
    }

    public void seekToFirst() {
        this.current = skipList.head.getLevelNext().get(0);
    }

    public void seekToLast() {
        this.current = skipList.tail.getLevel0PrevNode();
    }

    public void seek(T key) {
        this.current = skipList.findGreaterOrEqual(key, null);
    }
}
