package com.farmerworking.db.rabbitDb;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.Vector;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by John on 18/8/7.
 */
public class CodingTest {
    @Test
    public void testFixed32() {
        StringBuilder builder = new StringBuilder();
        for (int v = 0; v < 100000; v++) {
            Coding.putFixed32(builder, v);
        }
        Coding.putFixed32(builder, Integer.MAX_VALUE);
        Coding.putFixed32(builder, Integer.MIN_VALUE);

        char[] p = builder.toString().toCharArray();
        int offset = 0;
        for (int v = 0; v < 100000; v++) {
            assertEquals(v, Coding.decodeFixed32(p, offset).getRight().intValue());
            offset += 4;
        }
        assertEquals(Integer.MAX_VALUE, Coding.decodeFixed32(p, offset).getRight().intValue());
        offset += 4;
        assertEquals(Integer.MIN_VALUE, Coding.decodeFixed32(p, offset).getRight().intValue());
    }

    @Test
    public void testFixed64() {
        StringBuilder builder = new StringBuilder();
        for (int power = 0; power < 63; power++) {
            long v = 1 << power;
            Coding.putFixed64(builder, v - 1);
            Coding.putFixed64(builder, v);
            Coding.putFixed64(builder, v + 1);
        }
        Coding.putFixed64(builder, Long.MAX_VALUE);

        char[] p = builder.toString().toCharArray();
        int offset = 0;
        for (int power = 0; power < 63; power++) {
            long v = 1 << power;
            assertEquals(v - 1, Coding.decodeFixed64(p, offset).getRight().longValue());
            offset += 8;

            assertEquals(v, Coding.decodeFixed64(p, offset).getRight().longValue());
            offset += 8;

            assertEquals(v + 1, Coding.decodeFixed64(p, offset).getRight().longValue());
            offset += 8;
        }
        assertEquals(Long.MAX_VALUE, Coding.decodeFixed64(p, offset).getRight().longValue());
    }

    @Test
    // Test that encoding routines generate little-endian encodings
    public void testEncodingOutput() {
        StringBuilder builder = new StringBuilder();
        Coding.putFixed32(builder, 0x04030201);
        String dst = builder.toString();
        assertEquals(4, dst.length());
        assertEquals(0x01, dst.toCharArray()[0]);
        assertEquals(0x02, dst.toCharArray()[1]);
        assertEquals(0x03, dst.toCharArray()[2]);
        assertEquals(0x04, dst.toCharArray()[3]);

        builder = new StringBuilder();
        Coding.putFixed64(builder, 0x0807060504030201l);
        dst = builder.toString();

        assertEquals(8, dst.length());
        assertEquals(0x01, dst.toCharArray()[0]);
        assertEquals(0x02, dst.toCharArray()[1]);
        assertEquals(0x03, dst.toCharArray()[2]);
        assertEquals(0x04, dst.toCharArray()[3]);
        assertEquals(0x05, dst.toCharArray()[4]);
        assertEquals(0x06, dst.toCharArray()[5]);
        assertEquals(0x07, dst.toCharArray()[6]);
        assertEquals(0x08, dst.toCharArray()[7]);
    }

    @Test
    public void testVarint32() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 63; i++) {
            int v = (i / 32) << (i % 32);
            Coding.putVariant32(builder, v);
        }
        Coding.putVariant32(builder, Integer.MAX_VALUE);

        char[] chars = builder.toString().toCharArray();
        int index = 0;
        for (int i = 0; i < 63; i++) {
            int expected = ((i / 32) << (i % 32));
            Pair<Integer, Integer> result = Coding.decodeVariant32(chars, index);
            assertNotNull(result);
            assertEquals(expected, result.getRight().intValue());
            assertEquals(Coding.detectVariantLength(result.getRight().longValue()),
                    result.getLeft() - index);
            index = result.getLeft();
        }

        Pair<Integer, Integer> result = Coding.decodeVariant32(chars, index);
        assertNotNull(result);
        assertEquals(Integer.MAX_VALUE, result.getRight().intValue());
        assertEquals(Coding.detectVariantLength(result.getRight().longValue()),
                result.getLeft() - index);

        assertEquals(result.getLeft().intValue(), chars.length);
    }

    @Test
    public void testVarint64() {
        // Construct the list of values to check
        Vector<Long> values = new Vector<>();
        // Some special values
        values.add(0L);
        values.add(100L);
        values.add(Long.MAX_VALUE);
        values.add(Long.MAX_VALUE - 1);
        for (int k = 0; k < 31; k++) {
            // Test values near powers of two
            long power = 1 << k;
            values.add(power);
            values.add(power - 1);
            values.add(power + 1);
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            Coding.putVariant64(builder, values.get(i));
        }

        char[] chars = builder.toString().toCharArray();
        int index = 0;
        for (int i = 0; i < values.size(); i++) {
            assertTrue(index < chars.length);
            Pair<Integer, Long> pair = Coding.decodeVariant64(chars, index);
            assertNotNull(pair);
            assertEquals(values.get(i).longValue(), pair.getRight().longValue());
            assertEquals(Coding.detectVariantLength(pair.getRight()), pair.getLeft() - index);
            index = pair.getLeft();
        }
        assertEquals(index, chars.length);
    }

    @Test
    public void testVarint32Overflow() {
        String input = new String(new char[]{0x81, 0x82, 0x83, 0x84, 0x85, 0x11});
        assertNull(Coding.decodeVariant32(input.toCharArray(), 0));
    }

    @Test
    public void testVarint32Truncation() {
        StringBuilder builder = new StringBuilder();
        Coding.putVariant32(builder, Integer.MAX_VALUE);
        char[] chars = builder.toString().toCharArray();
        for (int len = 0; len < chars.length - 1; len++) {
            char[] target = new char[len];
            System.arraycopy(chars, 0, target, 0, len);
            assertNull(Coding.decodeVariant32(target, 0));
        }

        Pair<Integer, Integer> pair = Coding.decodeVariant32(chars, 0);
        assertNotNull(pair);
        assertEquals(Integer.MAX_VALUE, pair.getRight().intValue());
        assertEquals(chars.length, pair.getLeft().intValue());
    }

    @Test
    public void testVarint64Overflow() {
        String input = new String(new char[]{0x81, 0x82, 0x83, 0x84, 0x85, 0x81, 0x82, 0x83, 0x84, 0x85, 0x11});
        assertNull(Coding.decodeVariant64(input.toCharArray(), 0));
    }

    @Test
    public void testVarint64Truncation() {
        StringBuilder builder = new StringBuilder();
        Coding.putVariant64(builder, Long.MAX_VALUE);
        char[] chars = builder.toString().toCharArray();
        for (int len = 0; len < chars.length - 1; len++) {
            char[] target = new char[len];
            System.arraycopy(chars, 0, target, 0, len);
            assertNull(Coding.decodeVariant64(target, 0));
        }

        Pair<Integer, Long> pair = Coding.decodeVariant64(chars, 0);
        assertNotNull(pair);
        assertEquals(Long.MAX_VALUE, pair.getRight().longValue());
        assertEquals(chars.length, pair.getLeft().intValue());
    }

    @Test
    public void testStrings() {
        StringBuilder builder = new StringBuilder();
        Coding.putLengthPrefixedSlice(builder, new Slice(""));
        Coding.putLengthPrefixedSlice(builder, new Slice("foo"));
        Coding.putLengthPrefixedSlice(builder, new Slice("bar"));
        Coding.putLengthPrefixedSlice(builder, new Slice(StringUtils.repeat("x", 200)));

        String s = builder.toString();
        Pair<Integer, Slice> result = Coding.decodeLengthPrefixedSlice(s.toCharArray(), 0);
        assertNotNull(result);
        assertEquals("", result.getRight().toString());

        result = Coding.decodeLengthPrefixedSlice(s.toCharArray(), result.getLeft());
        assertNotNull(result);
        assertEquals("foo", result.getRight().toString());

        result = Coding.decodeLengthPrefixedSlice(s.toCharArray(), result.getLeft());
        assertNotNull(result);
        assertEquals("bar", result.getRight().toString());

        result = Coding.decodeLengthPrefixedSlice(s.toCharArray(), result.getLeft());
        assertNotNull(result);
        assertEquals(StringUtils.repeat("x", 200), result.getRight().toString());

        assertEquals(result.getLeft().intValue(), s.length());
    }
}