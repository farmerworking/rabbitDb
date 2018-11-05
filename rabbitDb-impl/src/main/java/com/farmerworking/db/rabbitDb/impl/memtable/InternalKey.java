package com.farmerworking.db.rabbitDb.impl.memtable;

import com.farmerworking.db.rabbitDb.impl.skiplist.Sizeable;
import lombok.Getter;

public class InternalKey implements Sizeable {

    private
    @Getter
    final String userKey;
    private
    @Getter
    final long sequence;
    private
    @Getter
    final ValueType valueType;

    public InternalKey(String userKey, long sequence, ValueType valueType) {
        this.userKey = userKey;
        this.sequence = sequence;
        this.valueType = valueType;
    }

    @Override
    public String toString(){
        return String.format("{%s, %d, %s}", userKey, sequence, valueType);
    }

    @Override
    public long approximateMemoryUsage() {
        return userKey.length();
    }
}
