package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import com.farmerworking.db.rabbitDb.api.Status;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

@Data
public class BlockHandle {
    public static int MAX_ENCODED_LENGTH = 2 * Coding.MAX_VARY_64_UNIT;

    private int offset;
    private int size;

    public BlockHandle() {}

    public BlockHandle(int offset, int size) {
        this.offset = offset;
        this.size = size;
    }

    public String encode() {
        StringBuilder builder = new StringBuilder();
        Coding.putVariant64(builder, offset);
        Coding.putVariant64(builder, size);
        return builder.toString();
    }

    public Pair<Status, Integer> decodeFrom(String slice) {
        char[] data = slice.toCharArray();
        Pair<Integer, Long> pair = Coding.decodeVariant64(data, 0);
        if (pair == null) {
            return Pair.of(Status.corruption("bad block handle"), 0);
        } else {
            this.offset = pair.getRight().intValue();
            pair = Coding.decodeVariant64(data, pair.getLeft());
            if (pair == null) {
                return Pair.of(Status.corruption("bad block handle"), 0);
            } else {
                this.size = pair.getRight().intValue();
                return Pair.of(Status.ok(), pair.getLeft());
            }
        }
    }
}
