package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.FilterPolicy;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;

public class FilterBlockReader extends FilterBlockBase{
    private char[] data;
    private int arrayOffset;
    private int num;
    private int base;
    private boolean malformed = false;
    private final FilterPolicy filterPolicy;

    public FilterBlockReader(FilterPolicy filterPolicy, String content) {
        this.filterPolicy = filterPolicy;

        // 1 byte for baseLg and 4 for start of offset array
        if (content.length() < 5) {
            this.malformed = true;
            return;
        }

        char[] data = content.toCharArray();
        Integer arrayOffset = Coding.decodeFixed32(data, content.length() - 5).getRight();

        if (arrayOffset > content.length() - 5) {
            this.malformed = true;
            return;
        }

        this.base = 1 << (int) content.charAt(content.length() - 1);
        this.data = data;
        this.arrayOffset = arrayOffset;
        this.num = (content.length() - arrayOffset - 5) / Coding.FIXED_32_UNIT;
    }

    public boolean keyMayMatch(long blockOffset, String key) {
        if (malformed) {
            return true; // Errors are treated as potential matches
        }

        int index = (int) blockOffsetToFilterIndex(blockOffset, base);
        if (index < num) {
            int filterOffset = Coding.decodeFixed32(data, arrayOffset + Coding.FIXED_32_UNIT * index).getRight();
            int nextFilterOffset = Coding.decodeFixed32(data, arrayOffset + Coding.FIXED_32_UNIT * (index + 1)).getRight();
            int length = nextFilterOffset - filterOffset;

            if (filterOffset <= nextFilterOffset && nextFilterOffset <= arrayOffset) {
                return filterPolicy.keyMayMatch(key, new String(data, filterOffset, length));
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
