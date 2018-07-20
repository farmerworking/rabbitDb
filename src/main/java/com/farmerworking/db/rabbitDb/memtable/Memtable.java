package com.farmerworking.db.rabbitDb.memtable;

import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.skiplist.SkipList;
import com.farmerworking.db.rabbitDb.skiplist.SkipListIterator;
import org.iq80.leveldb.DBComparator;

public class Memtable {
  private final InternalEntryComparator internalEntryComparator;
  private final SkipList<InternalEntry> table;

  public Memtable(DBComparator userComparator) {
    this.internalEntryComparator = new InternalEntryComparator(userComparator);
    this.table = new SkipList<>(internalEntryComparator);
  }

  public long approximateMemoryUsage() {
    return table.approximateMemoryUsage();
  }

  public MemtableIterator iterator() {
    return new MemtableIterator(table.iterator());
  }

  public void add(InternalKey internalKey, Slice value) {
    InternalEntry internalEntry = new InternalEntry(internalKey, value);
    table.insert(internalEntry);
  }

  public Slice get(InternalKey internalKey) {
    InternalEntry seekEntry = new InternalEntry(internalKey, null /* no need*/);
    SkipListIterator<InternalEntry> iterator = table.iterator();
    iterator.seek(seekEntry);
    if (iterator.valid()) {
      InternalEntry entry = iterator.key();

      if (this.internalEntryComparator.getUserComparator()
          .compare(entry.getInternalKey().getUserKey().getData(), internalKey.getUserKey().getData()) == 0) {
        if (entry.getInternalKey().getValueType() == ValueType.DELETE) {
          return null;
        } else {
          return entry.getValue();
        }
      } else {
        return null;
      }
    } else {
      return null;
    }
  }
}