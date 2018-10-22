package com.farmerworking.db.rabbitDb.sstable;

import com.farmerworking.db.rabbitDb.utils.Coding;
import com.farmerworking.db.rabbitDb.Slice;
import org.iq80.leveldb.DBComparator;

import java.util.ArrayList;

public class BlockBuilder {
    private final int blockRestartInterval;
    private final DBComparator comparator;

    private final ArrayList<Integer> restarts;
    private int counter;
    private boolean finished;
    private String lastKey;
    private StringBuilder buffer;

    public BlockBuilder(int blockRestartInterval, DBComparator comparator) {
        assert blockRestartInterval >= 1;
        this.blockRestartInterval = blockRestartInterval;
        this.comparator = comparator;

        this.finished = false;
        this.counter = 0;
        this.buffer = new StringBuilder();
        this.restarts = new ArrayList<>();
        this.restarts.add(0);
        this.lastKey = "";
    }

    public boolean isEmpty() {
        return buffer.length() == 0;
    }

    public void add(Slice key, Slice value) {
        assert !finished;
        assert counter <= this.blockRestartInterval;
        assert isEmpty() || this.comparator.compare(key.getBytes(), lastKey.getBytes()) > 0;

        int shared = 0;
        if (counter < this.blockRestartInterval) {
            while (shared < key.getSize() &&
                    shared < lastKey.length() &&
                    lastKey.charAt(shared) == key.get(shared)) {
                shared++;
            }
        } else {
            restarts.add(buffer.length());
            counter = 0;
        }

        lastKey = key.toString();
        counter++;

        Coding.putVariant32(buffer, shared);
        Coding.putVariant32(buffer, key.getSize() - shared);
        Coding.putVariant32(buffer, value.getSize());

        key.removePrefix(shared);
        buffer.append(key.getData());
        buffer.append(value.getData());
    }

    public Slice finish() {
        finished = true;

        for (Integer offset : restarts) {
            Coding.putFixed32(buffer, offset);
        }
        Coding.putFixed32(buffer, restarts.size());
        return new Slice(buffer.toString());
    }

    public void reset() {
        this.finished = false;
        this.counter = 0;
        this.buffer = new StringBuilder();
        this.restarts.clear();
        this.restarts.add(0);
        this.lastKey = "";
    }

    public int currentSizeEstimate() {
        if (finished) {
            return buffer.length();
        } else {
            return buffer.length() + Coding.FIXED_32_UNIT * restarts.size() + Coding.FIXED_32_UNIT;
        }
    }
}
