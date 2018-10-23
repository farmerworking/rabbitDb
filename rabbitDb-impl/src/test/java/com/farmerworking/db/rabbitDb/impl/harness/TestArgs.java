package com.farmerworking.db.rabbitDb.impl.harness;

import lombok.Data;

@Data
public class TestArgs {
    private String name;
    private boolean reverseCompare;
    private int restartInterval;

    public TestArgs(String name, boolean reverseCompare, int restartInterval) {
        this.name = name;
        this.reverseCompare = reverseCompare;
        this.restartInterval = restartInterval;
    }
}
