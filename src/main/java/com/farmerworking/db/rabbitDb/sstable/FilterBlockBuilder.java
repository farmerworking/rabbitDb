package com.farmerworking.db.rabbitDb.sstable;

import com.farmerworking.db.rabbitDb.Coding;
import com.farmerworking.db.rabbitDb.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class FilterBlockBuilder extends FilterBlockBase{
    protected static int baseLg = 11;
    protected static int base = 1 << baseLg;

    private final FilterPolicy filterPolicy;

    private Vector<Integer> filterOffsets;
    private List<Slice> keys;
    private StringBuilder result;

    public FilterBlockBuilder(FilterPolicy filterPolicy) {
        this.filterPolicy = filterPolicy;
        this.filterOffsets = new Vector<>();
        this.keys = new ArrayList<>();
        this.result = new StringBuilder();
    }

    public void startBlock(long blockOffset) {
        long filterIndex = blockOffsetToFilterIndex(blockOffset, base);
        assert filterIndex >= filterOffsets.size();
        while(filterIndex > filterOffsets.size()) {
            generateFilter();
        }
    }

    private void generateFilter() {
        filterOffsets.add(result.length());

        if (!this.keys.isEmpty()) {
            String filter = filterPolicy.createFilter(keys);
            result.append(filter);
            this.keys.clear();
        }
    }

    public void addKey(Slice key) {
        this.keys.add(key);
    }

    public Slice finish() {
        if (!this.keys.isEmpty()) {
            generateFilter();
        }

        int filterOffsetsArrayOffset = result.length();
        for(Integer offset : filterOffsets) {
            Coding.putFixed32(result, offset);
        }
        Coding.putFixed32(result, filterOffsetsArrayOffset);
        result.append((char)baseLg);
        return new Slice(result.toString());
    }
}
