package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.*;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import org.apache.commons.lang3.tuple.Pair;

public class Block {
    private final String blockContent;
    private boolean malformed;
    private int restartOffset;

    public Block(String blockContent) {
        this.blockContent = blockContent;
        if (blockContent.length() < Coding.FIXED_32_UNIT) {
            malformed = true;
        } else {
            int maxRestartsAllowed = (blockContent.length() - Coding.FIXED_32_UNIT) / Coding.FIXED_32_UNIT;

            if (getRestartsCount() > maxRestartsAllowed) {
                malformed = true;
            } else {
                this.restartOffset = blockContent.length() - (1 + getRestartsCount()) * Coding.FIXED_32_UNIT;
            }
        }
    }

    public DBIterator<String, String> iterator(DBComparator comparator) {
        if (malformed) {
            return new ErrorIterator<>(Status.corruption("bad block contents"));
        } else {
            if (getRestartsCount() == 0) {
                return new EmptyIterator<>();
            } else {
                return new BlockIterator(comparator, blockContent.toCharArray(), restartOffset, getRestartsCount());
            }
        }
    }

    private int getRestartsCount() {
        assert blockContent.length() >= Coding.FIXED_32_UNIT;
        Pair<Integer, Integer> pair = Coding
                .decodeFixed32(blockContent.toCharArray(), blockContent.length() - Coding.FIXED_32_UNIT);
        return pair.getRight();
    }

    String getBlockContent() {
        return blockContent;
    }
}
