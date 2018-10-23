package com.farmerworking.db.rabbitDb.impl.memtable;

import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.skiplist.Sizeable;
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
