package com.farmerworking.db.rabbitDb.sstable;

import com.farmerworking.db.rabbitDb.utils.Coding;
import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.util.Arrays;

@Data
public class Footer {
    public static int ENCODE_LENGTH = 2 * BlockHandle.MAX_ENCODED_LENGTH + 2 * Coding.FIXED_32_UNIT;
    private static BigInteger MAGIC_NUMBER = new BigInteger("15800726617472432983");

    private BlockHandle metaIndexHandle;
    private BlockHandle indexHandle;

    public void encodeTo(StringBuilder stringBuilder) {
        int originLength = stringBuilder.length();
        metaIndexHandle.encodeTo(stringBuilder);
        indexHandle.encodeTo(stringBuilder);

        // Padding
        int remain = 2 * BlockHandle.MAX_ENCODED_LENGTH - stringBuilder.length() + originLength;
        if (remain > 0) {
            char[] buffer = new char[remain];
            Arrays.fill(buffer, (char)0);
            stringBuilder.append(buffer);
        }

        Coding.putFixed32(stringBuilder, MAGIC_NUMBER.and(new BigInteger("4294967295")).intValue());
        Coding.putFixed32(stringBuilder, MAGIC_NUMBER.shiftRight(32).intValue());

        assert stringBuilder.length() == originLength + ENCODE_LENGTH;
    }

    public Status decodeFrom(Slice slice) {
        if (slice.getSize() < ENCODE_LENGTH) {
            return Status.corruption("bad footer");
        }

        char[] data = slice.getData();
        int magicOffset = slice.getSize() - 2 * Coding.FIXED_32_UNIT;
        Integer magicLo = Coding.decodeFixed32(data, magicOffset).getRight();
        Integer magicHi = Coding.decodeFixed32(data, magicOffset + Coding.FIXED_32_UNIT).getRight();

        BigInteger magic = new BigInteger(Integer.toUnsignedString(magicHi)).shiftLeft(32).or(new BigInteger(Integer.toUnsignedString(magicLo)));
        if (!magic.equals(MAGIC_NUMBER)) {
            return Status.corruption("not an sstable (bad magic number)");
        }

        this.metaIndexHandle = new BlockHandle();
        Pair<Status, Integer> pair = metaIndexHandle.decodeFrom(slice);
        if (pair.getLeft().isOk()) {
            this.indexHandle = new BlockHandle();
            pair = indexHandle.decodeFrom(new Slice(data, data.length - pair.getRight(), pair.getRight()));
        }

        return pair.getLeft();
    }
}
