package com.farmerworking.db.rabbitDb.impl.utils;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SnappyWrapperTest {

    @Test
    public void testCompressAndUncompress() {
        String content = "Hello snappy-java! Snappy-java is a JNI-based wrapper of Snappy, a fast compresser/decompresser.";

        SnappyWrapper snappyWrapper = new SnappyWrapper();
        byte[] compress1 = snappyWrapper.compress("");
        byte[] compress2 = snappyWrapper.compress("abc");
        byte[] compress3 = snappyWrapper.compress("123");
        byte[] compress4 = snappyWrapper.compress("中文");
        byte[] compress5 = snappyWrapper.compress("   ");
        byte[] compress6 = snappyWrapper.compress(content);

        String uncompress1 = snappyWrapper.uncompress(compress1);
        String uncompress2 = snappyWrapper.uncompress(compress2);
        String uncompress3 = snappyWrapper.uncompress(compress3);
        String uncompress4 = snappyWrapper.uncompress(compress4);
        String uncompress5 = snappyWrapper.uncompress(compress5);
        String uncompress6 = snappyWrapper.uncompress(compress6);

        assertEquals("", uncompress1);
        assertEquals("abc", uncompress2);
        assertEquals("123", uncompress3);
        assertEquals("中文", uncompress4);
        assertEquals("   ", uncompress5);
        assertEquals(content, uncompress6);

        byte[] levelDbCompress1 = new byte[]{0};
        byte[] levelDbCompress2 = new byte[]{3, 8, 97, 98, 99};
        byte[] levelDbCompress3 = new byte[]{3, 8, 49, 50, 51};
        byte[] levelDbCompress4 = new byte[]{6, 20, -28, -72, -83, -26, -106, -121};
        byte[] levelDbCompress5 = new byte[]{3, 8, 32, 32, 32};
        byte[] levelDbCompress6 = new byte[]{
                96, 76, 72, 101, 108, 108, 111, 32, 115, 110, 97, 112, 112, 121, 45, 106, 97, 118, 97, 33,
                32, 83, 25, 13, 100, 32,
                105, 115, 32, 97, 32, 74, 78, 73, 45, 98, 97, 115, 101, 100, 32, 119, 114, 97, 112, 112,
                101, 114, 32, 111, 102, 13,
                38, -128, 44, 32, 97, 32, 102, 97, 115, 116, 32, 99, 111, 109, 112, 114, 101, 115, 115, 101,
                114, 47, 100, 101, 99,
                111, 109, 112, 114, 101, 115, 115, 101, 114, 46};

        assertArrayEquals(levelDbCompress1, compress1);
        assertArrayEquals(levelDbCompress2, compress2);
        assertArrayEquals(levelDbCompress3, compress3);
        assertArrayEquals(levelDbCompress4, compress4);
        assertArrayEquals(levelDbCompress5, compress5);
        assertArrayEquals(levelDbCompress6, compress6);
    }
}