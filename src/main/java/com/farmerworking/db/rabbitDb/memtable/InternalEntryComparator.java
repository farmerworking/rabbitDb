package com.farmerworking.db.rabbitDb.memtable;

import java.util.Comparator;
import lombok.Getter;
import org.iq80.leveldb.DBComparator;

class InternalEntryComparator implements Comparator<InternalEntry> {

  private
  @Getter
  DBComparator userComparator;

  public InternalEntryComparator(DBComparator userComparator) {
    this.userComparator = userComparator;
  }

  public int compare(InternalEntry o1, InternalEntry o2) {
    int compareResult = userComparator
        .compare(o1.getInternalKey().getUserKey().getData(), o2.getInternalKey().getUserKey().getData());

    if (compareResult == 0) {
      return Long.compare(o2.getInternalKey().getSequence(), o1.getInternalKey().getSequence());
    } else {
      return compareResult;
    }
  }
}