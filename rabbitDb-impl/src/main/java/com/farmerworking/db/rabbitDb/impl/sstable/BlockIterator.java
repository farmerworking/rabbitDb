package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import com.farmerworking.db.rabbitDb.api.Status;
import org.apache.commons.lang3.tuple.Pair;

public class BlockIterator implements DBIterator<String, String> {

    private DBComparator comparator;
    private char[] data;

    private int restartOffset;
    private int restartsCount;

    private int current;
    private int restartIndex;

    private Status status;
    private String key;
    private Pair<Integer, Integer> value; // offset + size;

    public BlockIterator(DBComparator comparator, char[] data, int restartOffset, int restartsCount) {
        this.comparator = comparator;
        this.data = data;
        this.restartOffset = restartOffset;
        this.restartsCount = restartsCount;

        // put point to the end of region block(data region, restart region)
        this.current = restartOffset;
        this.restartIndex = restartsCount;

        this.status = Status.ok();
    }

    public Status getStatus() {
        return status;
    }


    public boolean isValid() {
        return status.isOk() && current < restartOffset;
    }

    public String key() {
        assert (isValid());
        return key;
    }

    public String value() {
        assert (isValid());

        int offset = value.getLeft();
        int length = value.getRight();
        return new String(data, offset, length);
    }

    public void next() {
        assert isValid();
        parseNextKey();
    }

    public void prev() {
        assert isValid();

        // Scan backwards to a restart point before current_
        int original = current;
        while (getRestartPoint(restartIndex) >= original) {
            if (restartIndex == 0) {
                current = restartOffset;
                restartIndex = restartsCount;
                return;
            }

            restartIndex--;
        }

        seekToRestartPoint(restartIndex);
        while (parseNextKey() && nextEntryOffset() < original) {
            // nothing
        }
    }

    public void seekToFirst() {
        assert status.isOk();
        seekToRestartPoint(0);
        parseNextKey();
    }

    public void seekToLast() {
        assert status.isOk();
        seekToRestartPoint(restartsCount - 1);
        while (parseNextKey() && nextEntryOffset() < restartOffset) {
            // Keep skipping
        }
    }

    @Override
    public void seek(String target) {
        assert status.isOk();
        // Binary search in restart array to find the last restart point
        // with a key < target
        int left = 0;
        int right = restartsCount - 1;
        while (left < right) {
            int mid = (left + right + 1) / 2;
            int regionOffset = getRestartPoint(mid);
            Pair<Integer, Pair<Integer, Pair<Integer, Integer>>> pair = decodeEntry(data, regionOffset,
                    restartOffset);
            if (pair == null) {
                corruptionError();
                return;
            }

            int offset = pair.getLeft();
            int shared = pair.getRight().getLeft();
            int nonShared = pair.getRight().getRight().getLeft();
            if (shared != 0) {
                corruptionError();
                return;
            }

            String middleKey = new String(data, offset, nonShared);
            if (this.comparator.compare(middleKey, target) < 0) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            }
        }

        // Linear search (within restart block) for first key >= target
        seekToRestartPoint(left);
        while (true) {
            if (!parseNextKey()) {
                return;
            }
            if (this.comparator.compare(key, target) >= 0) {
                return;
            }
        }

    }

    private void corruptionError() {
        current = restartOffset;
        restartIndex = restartsCount;
        status = Status.corruption("bad entry in block");
        key = null;
        value = null;
    }

    private boolean parseNextKey() {
        current = nextEntryOffset();
        if (current >= restartOffset) {
            // No more entries to return.  Mark as invalid.
            current = restartOffset;
            restartIndex = restartsCount;
            return false;
        }

        // Decode next entry
        Pair<Integer, Pair<Integer, Pair<Integer, Integer>>> pair = decodeEntry(data, current,
                restartOffset);

        if (pair == null) {
            corruptionError();
            return false;
        }

        Integer offset = pair.getLeft();
        Integer shared = pair.getRight().getLeft();
        Integer nonShared = pair.getRight().getRight().getLeft();
        Integer valueLength = pair.getRight().getRight().getRight();

        if (key.length() < shared) {
            corruptionError();
            return false;
        }

        key =  key.substring(0, shared) + new String(data, offset, nonShared);
        value = Pair.of(offset + nonShared, valueLength);
        while (restartIndex + 1 < restartsCount && getRestartPoint(restartIndex + 1) < current) {
            ++restartIndex;
        }
        return true;
    }

    private void seekToRestartPoint(int restartIndex) {
        key = "";
        this.restartIndex = restartIndex;

        int offset = getRestartPoint(restartIndex);
        value = Pair.of(offset, 0);
    }

    private int getRestartPoint(int index) {
        assert index < restartsCount && index >= 0;
        return Coding.decodeFixed32(data, restartOffset + index * Coding.FIXED_32_UNIT).getRight();
    }

    // Return the offset in data_ just past the end of the current entry.
    private int nextEntryOffset() {
        return value.getLeft() + value.getRight();
    }

    private Pair<Integer, Pair<Integer, Pair<Integer, Integer>>> decodeEntry(char[] buffer,
                                                                             int offset, int limit) {
        if (limit - offset < 3) {
            return null;
        }

        int shared = buffer[offset];
        int nonShared = buffer[offset + 1];
        int valueLength = buffer[offset + 2];
        if ((shared | nonShared | valueLength) < 128) {
            // Fast path: all three values are encoded in one byte each
            offset += 3;
        } else {
            Pair<Integer, Integer> pair = Coding.decodeVariant32(buffer, offset);
            if (pair == null) {
                return null;
            }
            offset = pair.getLeft();
            shared = pair.getRight();

            pair = Coding.decodeVariant32(buffer, offset);
            if (pair == null) {
                return null;
            }
            offset = pair.getLeft();
            nonShared = pair.getRight();

            pair = Coding.decodeVariant32(buffer, offset);
            if (pair == null) {
                return null;
            }
            offset = pair.getLeft();
            valueLength = pair.getRight();
        }

        if ((limit - offset) < (nonShared + valueLength)) {
            return null;
        }
        return Pair.of(offset, Pair.of(shared, Pair.of(nonShared, valueLength)));
    }
}
