package com.farmerworking.db.rabbitDb.memtable;

import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.skiplist.Sizeable;
import lombok.Getter;

public class InternalEntry implements Sizeable {

    private @Getter
    final InternalKey internalKey;
    private @Getter
    final Slice value;

    InternalEntry(InternalKey internalKey, Slice value) {
        this.internalKey = internalKey;
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("{%s, %s}", internalKey.toString(), value.toString());
    }

    @Override
    public long approximateMemoryUsage() {
        return internalKey.approximateMemoryUsage() + value.getSize();
    }
}
