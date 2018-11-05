package com.farmerworking.db.rabbitDb.impl.memtable;

import com.farmerworking.db.rabbitDb.impl.skiplist.Sizeable;
import lombok.Getter;

public class InternalEntry implements Sizeable {

    private @Getter
    final InternalKey internalKey;
    private @Getter
    final String value;

    InternalEntry(InternalKey internalKey, String value) {
        this.internalKey = internalKey;
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("{%s, %s}", internalKey, value);
    }

    @Override
    public long approximateMemoryUsage() {
        return internalKey.approximateMemoryUsage() + value.length();
    }
}
