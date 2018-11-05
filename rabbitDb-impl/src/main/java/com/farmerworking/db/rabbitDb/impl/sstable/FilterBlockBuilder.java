package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.FilterPolicy;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class FilterBlockBuilder extends FilterBlockBase{
    protected static int baseLg = 11;
    protected static int base = 1 << baseLg;

    private final FilterPolicy filterPolicy;

    private Vector<Integer> filterOffsets;
    private List<String> keys;
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

    public void addKey(String key) {
        this.keys.add(key);
    }

    public String finish() {
        if (!this.keys.isEmpty()) {
            generateFilter();
        }

        int filterOffsetsArrayOffset = result.length();
        for(Integer offset : filterOffsets) {
            Coding.putFixed32(result, offset);
        }
        Coding.putFixed32(result, filterOffsetsArrayOffset);
        result.append((char)baseLg);
        return result.toString();
    }
}
