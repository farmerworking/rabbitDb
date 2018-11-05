package com.farmerworking.db.rabbitDb.impl.writebatch;

import lombok.Getter;

class WriteBatchItem {

    private
    @Getter
    String key;
    private String value;

    WriteBatchItem(String key) {
        this.key = key;
    }

    WriteBatchItem(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public boolean isDelete() {
        return value == null;
    }

    public String getValue() {
        assert !isDelete();
        return value;
    }
}
