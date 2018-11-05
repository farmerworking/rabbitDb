package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import com.farmerworking.db.rabbitDb.api.Status;
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

    public String encode() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(metaIndexHandle.encode());
        stringBuilder.append(indexHandle.encode());

        // Padding
        int remain = 2 * BlockHandle.MAX_ENCODED_LENGTH - stringBuilder.length();
        if (remain > 0) {
            char[] buffer = new char[remain];
            Arrays.fill(buffer, (char)0);
            stringBuilder.append(buffer);
        }

        Coding.putFixed32(stringBuilder, MAGIC_NUMBER.and(new BigInteger("4294967295")).intValue());
        Coding.putFixed32(stringBuilder, MAGIC_NUMBER.shiftRight(32).intValue());

        assert stringBuilder.length() ==  ENCODE_LENGTH;
        return stringBuilder.toString();
    }

    public Status decodeFrom(String slice) {
        if (slice.length() < ENCODE_LENGTH) {
            return Status.corruption("bad footer");
        }

        char[] data = slice.toCharArray();
        int magicOffset = slice.length() - 2 * Coding.FIXED_32_UNIT;
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
            pair = indexHandle.decodeFrom(new String(data, pair.getRight(), data.length - pair.getRight()));
        }

        return pair.getLeft();
    }
}
