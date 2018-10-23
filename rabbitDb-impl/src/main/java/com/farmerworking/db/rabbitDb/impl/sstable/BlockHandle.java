package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.api.Status;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

@Data
public class BlockHandle {
    public static int MAX_ENCODED_LENGTH = 2 * Coding.MAX_VARY_64_UNIT;

    private long offset;
    private long size;

    public BlockHandle() {}

    public BlockHandle(long offset, long size) {
        this.offset = offset;
        this.size = size;
    }

    public void encodeTo(StringBuilder stringBuilder) {
        assert offset != 0L;
        assert size != 0L;

        Coding.putVariant64(stringBuilder, offset);
        Coding.putVariant64(stringBuilder, size);
    }

    public Pair<Status, Integer> decodeFrom(Slice slice) {
        char[] data = slice.getData();
        Pair<Integer, Long> pair = Coding.decodeVariant64(data, 0);
        if (pair == null) {
            return Pair.of(Status.corruption("bad block handle"), 0);
        } else {
            this.offset = pair.getRight();
            pair = Coding.decodeVariant64(data, pair.getLeft());
            if (pair == null) {
                return Pair.of(Status.corruption("bad block handle"), 0);
            } else {
                this.size = pair.getRight();
                return Pair.of(Status.ok(), pair.getLeft());
            }
        }
    }
}
