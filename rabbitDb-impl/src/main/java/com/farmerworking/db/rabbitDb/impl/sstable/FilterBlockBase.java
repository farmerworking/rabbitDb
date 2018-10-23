package com.farmerworking.db.rabbitDb.impl.sstable;

public abstract class FilterBlockBase {

    protected long blockOffsetToFilterIndex(long blockOffset, int base) {
        return blockOffset / base;
    }
}
