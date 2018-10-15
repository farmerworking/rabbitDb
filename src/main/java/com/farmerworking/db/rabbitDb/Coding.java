package com.farmerworking.db.rabbitDb;

import org.apache.commons.lang3.tuple.Pair;

public class Coding {

    public static final int FIXED_32_UNIT = 4;
    public static final int FIXED_64_UNIT = 8;
    public static final int MAX_VARY_64_UNIT = 10;
    public static final int MAX_VARY_32_UNIT = 5;

    public static Pair<Integer, Integer> decodeFixed32(char[] buf, int offset) {
        return Pair.of(offset + 4, buf[offset] |
                buf[offset + 1] << 8 |
                buf[offset + 2] << 16 |
                buf[offset + 3] << 24);
    }

    public static int encodeFixed32(char[] buf, int offset, int value) {
        assert buf.length >= offset + 4;
        buf[offset] = (char) (value & 0xff);
        buf[offset + 1] = (char) ((value >> 8) & 0xff);
        buf[offset + 2] = (char) ((value >> 16) & 0xff);
        buf[offset + 3] = (char) ((value >> 24) & 0xff);
        return offset + 4;
    }

    public static void putFixed32(StringBuilder builder, int value) {
        char[] buf = new char[4];
        encodeFixed32(buf, 0, value);
        builder.append(buf);
    }

    public static Pair<Integer, Long> decodeFixed64(char[] buf, int offset) {
        return Pair.of(offset + 8, ((long) buf[offset])
                | ((long) buf[offset + 1] << 8)
                | ((long) buf[offset + 2] << 16)
                | ((long) buf[offset + 3] << 24)
                | ((long) buf[offset + 4] << 32)
                | ((long) buf[offset + 5] << 40)
                | ((long) buf[offset + 6] << 48)
                | ((long) buf[offset + 7] << 56));
    }

    public static int encodeFixed64(char[] buf, int offset, long value) {
        assert buf.length >= offset + 8;
        buf[offset] = (char) (value & 255);
        buf[offset + 1] = (char) ((value >> 8) & 255);
        buf[offset + 2] = (char) ((value >> 16) & 255);
        buf[offset + 3] = (char) ((value >> 24) & 255);
        buf[offset + 4] = (char) ((value >> 32) & 255);
        buf[offset + 5] = (char) ((value >> 40) & 255);
        buf[offset + 6] = (char) ((value >> 48) & 255);
        buf[offset + 7] = (char) ((value >> 56) & 255);
        return offset + 8;
    }

    public static void putFixed64(StringBuilder builder, long value) {
        char[] buf = new char[8];
        encodeFixed64(buf, 0, value);
        builder.append(buf);
    }

    public static Pair<Integer, Integer> decodeVariant32(char[] buf, int offset) {
        Pair<Integer, Long> pair = decodeVariantInternal(buf, offset, 29);
        if (pair == null) {
            return null;
        } else {
            return Pair.of(pair.getLeft(), pair.getRight().intValue());
        }
    }

    // invariant: the first bit of right most byte will be 0 otherwise will be 1
    private static int encodeVariant32(char[] buf, int offset, int revisedValue) {
        while (revisedValue >= 128) {
            buf[offset++] = (char) (revisedValue | 128);
            revisedValue = revisedValue >> 7;
        }

        buf[offset++] = (char) revisedValue;
        return offset;
    }

    public static int putVariant32(StringBuilder builder, int value) {
        int length = detectVariantLength(value);
        char[] buf = new char[length];
        encodeVariant32(buf, 0, value);
        builder.append(buf);
        return length;
    }

    public static Pair<Integer, Long> decodeVariant64(char[] buf, int offset) {
        return decodeVariantInternal(buf, offset, 64);

    }

    public static int encodeVariant64(char[] buf, int offset, long value) {
        while (value >= 128) {
            buf[offset++] = (char) (value | 128);
            value = value >> 7;
        }

        buf[offset++] = (char) value;
        return offset;
    }

    public static int putVariant64(StringBuilder builder, long value) {
        int length = detectVariantLength(value);
        char[] buf = new char[length];
        encodeVariant64(buf, 0, value);
        builder.append(buf);
        return length;
    }

    public static Pair<Integer, Slice> decodeLengthPrefixedSlice(char[] buf, int offset) {
        Pair<Integer, Integer> decodeResult = decodeVariant32(buf, offset);
        if (decodeResult == null) {
            return null;
        } else {
            int length = decodeResult.getRight();
            int lengthOfLength = detectVariantLength(length);

            if (length + offset + lengthOfLength > buf.length) {
                return null;
            } else {
                return Pair.of(decodeResult.getLeft() + length,
                        new Slice(buf, length, decodeResult.getLeft()));
            }
        }
    }

    public static void putLengthPrefixedSlice(StringBuilder builder, Slice value) {
        putVariant32(builder, value.getSize());
        builder.append(value.getData());
    }

    public static int detectVariantLength(Long value) {
        int len = 1;
        while (value >= 128) {
            value = value >> 7;
            len++;
        }

        return len;
    }

    private static int detectVariantLength(long value) {
        int len = 1;
        while (value >= 128) {
            value = value >> 7;
            len++;
        }

        return len;
    }

    private static Pair<Integer, Long> decodeVariantInternal(char[] buf, int offset,
                                                             int shiftLimit) {
        long result = 0L;
        for (int shift = 0; shift < shiftLimit && offset < buf.length; shift += 7) {
            long value = buf[offset];
            offset++;
            if ((value & 128) != 0) {
                // More bytes are present
                result = result | ((value & 127) << shift);
            } else {
                result = result | (value << shift);
                return Pair.of(offset, result);
            }
        }
        return null;
    }
}
