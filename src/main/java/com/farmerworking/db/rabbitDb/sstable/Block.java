package com.farmerworking.db.rabbitDb.sstable;

import com.farmerworking.db.rabbitDb.Coding;
import com.farmerworking.db.rabbitDb.DBIterator;
import com.farmerworking.db.rabbitDb.EmptyIterator;
import com.farmerworking.db.rabbitDb.ErrorIterator;
import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;
import org.apache.commons.lang3.tuple.Pair;
import org.iq80.leveldb.DBComparator;

public class Block {
  private final Slice blockContent;
  private boolean malformed;
  private int restartOffset;

  public Block(Slice blockContent) {
    this.blockContent = blockContent;
    if (blockContent.getSize() < Coding.FIXED_32_UNIT) {
      malformed = true;
    } else {
      int maxRestartsAllowed = (blockContent.getSize() - Coding.FIXED_32_UNIT) / Coding.FIXED_32_UNIT;

      if (getRestartsCount() > maxRestartsAllowed) {
        malformed = true;
      } else {
        this.restartOffset = blockContent.getSize() - (1 + getRestartsCount()) * Coding.FIXED_32_UNIT;
      }
    }
  }

  DBIterator<Slice, Slice> iterator(DBComparator comparator) {
    if (malformed) {
      return new ErrorIterator<>(Status.corruption("bad block contents"));
    } else {
      if (getRestartsCount() == 0) {
        return new EmptyIterator<>();
      } else {
        return new BlockIterator(comparator, blockContent.getData(), restartOffset, getRestartsCount());
      }
    }
  }

  private int getRestartsCount() {
    assert blockContent.getSize() >= Coding.FIXED_32_UNIT;
    Pair<Integer, Integer> pair = Coding
        .decodeFixed32(blockContent.getData(), blockContent.getSize() - Coding.FIXED_32_UNIT);
    return pair.getRight();
  }
}
