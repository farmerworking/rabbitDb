package com.farmerworking.db.rabbitDb.memtable;

import lombok.Getter;
import org.iq80.leveldb.DBComparator;

import java.util.Comparator;

class InternalEntryComparator implements Comparator<InternalEntry> {

    private
    @Getter
    DBComparator userComparator;

    public InternalEntryComparator(DBComparator userComparator) {
        this.userComparator = userComparator;
    }

    public int compare(InternalEntry o1, InternalEntry o2) {
        int compareResult = userComparator
                .compare(o1.getInternalKey().getUserKey().getBytes(),
                        o2.getInternalKey().getUserKey().getBytes());

        if (compareResult == 0) {
            return Long.compare(o2.getInternalKey().getSequence(), o1.getInternalKey().getSequence());
        } else {
            return compareResult;
        }
    }
}
