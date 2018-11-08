package com.farmerworking.db.rabbitDb.impl.memtable;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import lombok.Getter;

import java.util.Comparator;

public class InternalEntryComparator implements Comparator<InternalEntry> {

    private
    @Getter
    DBComparator userComparator;

    public InternalEntryComparator(DBComparator userComparator) {
        this.userComparator = userComparator;
    }

    public int compare(InternalEntry o1, InternalEntry o2) {
        return compare(o1.getInternalKey(), o2.getInternalKey());
    }

    public int compare(InternalKey o1, InternalKey o2) {
        int compareResult = userComparator.compare(o1.getUserKey(), o2.getUserKey());

        if (compareResult == 0) {
            return Long.compare(o2.getSequence(), o1.getSequence());
        } else {
            return compareResult;
        }
    }
}
