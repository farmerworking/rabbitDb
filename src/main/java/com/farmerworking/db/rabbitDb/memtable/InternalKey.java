package com.farmerworking.db.rabbitDb.memtable;

import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.skiplist.Sizeable;
import lombok.Getter;

public class InternalKey implements Sizeable {

    private
    @Getter
    final Slice userKey;
    private
    @Getter
    final long sequence;
    private
    @Getter
    final ValueType valueType;

    public InternalKey(Slice userKey, long sequence, ValueType valueType) {
        this.userKey = userKey;
        this.sequence = sequence;
        this.valueType = valueType;
    }

    @Override
    public String toString() {
        return String.format("{%s, %d, %s}", userKey.toString(), sequence, valueType);
    }

    @Override
    public long approximateMemoryUsage() {
        return userKey.getSize();
    }
}
