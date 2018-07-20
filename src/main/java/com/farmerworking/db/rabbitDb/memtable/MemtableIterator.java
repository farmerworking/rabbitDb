package com.farmerworking.db.rabbitDb.memtable;

import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.skiplist.SkipListIterator;

public class MemtableIterator {

  private final SkipListIterator<InternalEntry> iterator;

  MemtableIterator(SkipListIterator<InternalEntry> iterator) {
    this.iterator = iterator;
  }

  public boolean valid() {
    return iterator.valid();
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
