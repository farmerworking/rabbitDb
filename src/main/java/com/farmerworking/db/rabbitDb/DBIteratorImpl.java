package com.farmerworking.db.rabbitDb;

import com.farmerworking.db.rabbitDb.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.memtable.MemtableIterator;
import com.farmerworking.db.rabbitDb.memtable.ValueType;
import org.iq80.leveldb.DBComparator;

public class DBIteratorImpl {

  private final MemtableIterator memtableIterator;
  private final long sequence;
  private final DBComparator comparator;

  public DBIteratorImpl(MemtableIterator memtableIterator, long sequence,
      DBComparator comparator) {
    this.memtableIterator = memtableIterator;
    this.sequence = sequence;
    this.comparator = comparator;
  }

  public Slice key() {
    return memtableIterator.key().getUserKey();
  }

  public Slice value() {
    return memtableIterator.value();
  }

  public boolean valid() {
    return memtableIterator.valid();
  }

  public void seekToFirst() {
    memtableIterator.seekToFirst();
    nextValidUserKey(null);
  }

  public void seek(Slice key) {
    memtableIterator.seek(new InternalKey(key, sequence, null));
    nextValidUserKey(null);
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
    while (memtableIterator.valid()) {
      InternalKey internalKey = memtableIterator.key();

      if (internalKey.getSequence() > sequence) {
        // skip new record since this iterator creation
        memtableIterator.prev();
      } else if (currentKey == null ||
          comparator.compare(currentKey.getUserKey().getData(), internalKey.getUserKey().getData())
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

  private void nextValidUserKey(Slice deletedKey) {
    while (memtableIterator.valid()) {
      InternalKey internalKey = memtableIterator.key();

      if (internalKey.getSequence() > sequence) {
        // skip new record since this iterator creation
        memtableIterator.next();
      } else if (internalKey.getValueType() == ValueType.DELETE) {
        deletedKey = internalKey.getUserKey();
        memtableIterator.next();
      } else if (deletedKey != null
          && comparator.compare(deletedKey.getData(), internalKey.getUserKey().getData()) == 0) {
        memtableIterator.next();
      } else {
        break;
      }
    }
  }
}