package com.farmerworking.db.rabbitDb.sstable;

public abstract class FilterBlockBase {

    protected long blockOffsetToFilterIndex(long blockOffset, int base) {
        return blockOffset / base;
    }
}
