package com.farmerworking.db.rabbitDb.impl.harness;

import com.farmerworking.db.rabbitDb.api.FilterPolicy;
import lombok.Data;

@Data
public class TestArgs {
    private String name;
    private boolean reverseCompare;
    private int restartInterval;
    // only affect table
    private boolean compress = false;
    private FilterPolicy filterPolicy;

    public TestArgs(String name, boolean reverseCompare, int restartInterval) {
        this.name = name;
        this.reverseCompare = reverseCompare;
        this.restartInterval = restartInterval;
    }

    public TestArgs(String name, boolean reverseCompare, int restartInterval, FilterPolicy filterPolicy) {
        this.name = name;
        this.reverseCompare = reverseCompare;
        this.restartInterval = restartInterval;
        this.filterPolicy = filterPolicy;
    }

    public TestArgs(String name, boolean reverseCompare, int restartInterval, boolean compress) {
        this.name = name;
        this.reverseCompare = reverseCompare;
        this.restartInterval = restartInterval;
        this.compress = compress;
    }

    public TestArgs(String name, boolean reverseCompare, int restartInterval, boolean compress, FilterPolicy filterPolicy) {
        this.name = name;
        this.reverseCompare = reverseCompare;
        this.restartInterval = restartInterval;
        this.compress = compress;
        this.filterPolicy = filterPolicy;
    }
}
