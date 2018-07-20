package com.farmerworking.db.rabbitDb.writebatch;

import com.farmerworking.db.rabbitDb.Slice;
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
