package com.farmerworking.db.rabbitDb.impl.writebatch;

import com.farmerworking.db.rabbitDb.impl.Slice;
import lombok.Getter;

class WriteBatchItem {

    private
    @Getter
    Slice key;
    private Slice value;

    WriteBatchItem(Slice key) {
        this.key = key;
    }

    WriteBatchItem(Slice key, Slice value) {
        this.key = key;
        this.value = value;
    }

    public boolean isDelete() {
        return value == null;
    }

    public Slice getValue() {
        assert !isDelete();
        return value;
    }
}
