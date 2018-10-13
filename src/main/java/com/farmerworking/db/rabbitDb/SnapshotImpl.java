package com.farmerworking.db.rabbitDb;

import lombok.Getter;
import lombok.Setter;
import org.iq80.leveldb.Snapshot;

public class SnapshotImpl implements Snapshot {

    private
    @Setter
    @Getter
    long sequence;

    public SnapshotImpl(long sequence) {
        this.sequence = sequence;
    }

    @Override
    public void close() {
    }
}
