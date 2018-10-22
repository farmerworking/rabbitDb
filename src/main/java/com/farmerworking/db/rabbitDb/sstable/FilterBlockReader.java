package com.farmerworking.db.rabbitDb.sstable;

import com.farmerworking.db.rabbitDb.utils.Coding;
import com.farmerworking.db.rabbitDb.Slice;

public class FilterBlockReader extends FilterBlockBase{
    private char[] data;
    private int arrayOffset;
    private int num;
    private int base;
    private boolean malformed = false;
    private final FilterPolicy filterPolicy;

    public FilterBlockReader(FilterPolicy filterPolicy, Slice content) {
        this.filterPolicy = filterPolicy;

        // 1 byte for baseLg and 4 for start of offset array
        if (content.getSize() < 5) {
            this.malformed = true;
            return;
        }

        char[] data = content.getData();
        Integer arrayOffset = Coding.decodeFixed32(data, content.getSize() - 5).getRight();

        if (arrayOffset > content.getSize() - 5) {
            this.malformed = true;
            return;
        }

        this.base = 1 << (int) content.get(content.getSize() - 1);
        this.data = data;
        this.arrayOffset = arrayOffset;
        this.num = (content.getSize() - arrayOffset - 5) / Coding.FIXED_32_UNIT;
    }

    public boolean keyMayMatch(long blockOffset, Slice key) {
        if (malformed) {
            return true; // Errors are treated as potential matches
        }

        int index = (int) blockOffsetToFilterIndex(blockOffset, base);
        if (index < num) {
            int filterOffset = Coding.decodeFixed32(data, arrayOffset + Coding.FIXED_32_UNIT * index).getRight();
            int nextFilterOffset = Coding.decodeFixed32(data, arrayOffset + Coding.FIXED_32_UNIT * (index + 1)).getRight();
            int length = nextFilterOffset - filterOffset;

            if (filterOffset <= nextFilterOffset && nextFilterOffset <= arrayOffset) {
                return filterPolicy.keyMayMatch(key, new Slice(data, length, filterOffset));
            } else if (filterOffset == nextFilterOffset) {
                // Empty filters do not match any keys
                return false;
            } else {
                return true; // Errors are treated as potential matches
            }
        } else {
            return true; // Errors are treated as potential matches
        }
    }
}
