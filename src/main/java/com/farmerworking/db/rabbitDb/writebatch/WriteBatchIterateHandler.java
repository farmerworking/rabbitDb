package com.farmerworking.db.rabbitDb.writebatch;

import com.farmerworking.db.rabbitDb.Slice;

public interface WriteBatchIterateHandler {

  void put(Slice key, Slice value);

  void delete(Slice key);

  void setSequence(long sequence);
}
