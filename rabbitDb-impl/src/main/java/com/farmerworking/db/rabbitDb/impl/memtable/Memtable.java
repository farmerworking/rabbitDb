package com.farmerworking.db.rabbitDb.impl.memtable;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.impl.skiplist.SkipList;
import com.farmerworking.db.rabbitDb.impl.skiplist.SkipListIterator;

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

    public void add(InternalKey internalKey, String value) {
        InternalEntry internalEntry = new InternalEntry(internalKey, value);
        table.insert(internalEntry);
    }

    public String get(InternalKey internalKey) {
        InternalEntry seekEntry = new InternalEntry(internalKey, null /* no need*/);
        SkipListIterator<InternalEntry> iterator = table.iterator();
        iterator.seek(seekEntry);
        if (iterator.isValid()) {
            InternalEntry entry = iterator.key();

            if (this.internalEntryComparator.getUserComparator()
                    .compare(entry.getInternalKey().getUserKey().toCharArray(), internalKey.getUserKey().toCharArray()) == 0) {
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
