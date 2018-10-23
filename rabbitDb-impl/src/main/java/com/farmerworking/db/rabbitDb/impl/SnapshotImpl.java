package com.farmerworking.db.rabbitDb.impl;

import com.farmerworking.db.rabbitDb.api.Snapshot;
import lombok.Getter;
import lombok.Setter;

public class SnapshotImpl implements Snapshot {

    private
    @Setter
    @Getter
    long sequence;

    public SnapshotImpl(long sequence) {
        this.sequence = sequence;
    }
}
