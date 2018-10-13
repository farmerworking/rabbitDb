package com.farmerworking.db.rabbitDb;

import org.iq80.leveldb.DBComparator;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by John on 18/7/11.
 */
public class ByteWiseComparatorTest {

    @Test
    public void testCompare() throws Exception {
        DBComparator comparator = ByteWiseComparator.getInstance();
        assertEquals(0, comparator.compare("b".getBytes(), "b".getBytes()));
        assertEquals(-1, comparator.compare("b".getBytes(), "c".getBytes()));
        assertEquals(1, comparator.compare("b".getBytes(), "a".getBytes()));
    }

    @Test
    public void testFindShortSuccessor() throws Exception {
        DBComparator comparator = ByteWiseComparator.getInstance();
        assertArrayEquals("b".getBytes(), comparator.findShortSuccessor("abcdef".getBytes()));

        assertArrayEquals(new byte[]{Byte.MAX_VALUE},
                comparator.findShortSuccessor(new byte[]{Byte.MAX_VALUE}));
        assertArrayEquals(new byte[]{Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, "b".getBytes()[0]},
                comparator.findShortSuccessor(
                        new byte[]{Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, "a".getBytes()[0],
                                "b".getBytes()[0]}));
    }

    @Test
    public void testFindShortestSeparator() throws Exception {
        DBComparator comparator = ByteWiseComparator.getInstance();

        // prefix
        assertArrayEquals("abcdef".getBytes(), comparator.findShortestSeparator("abcdef".getBytes(),
                "abcdefghijk".getBytes()));
        assertArrayEquals("abcdef".getBytes(),
                comparator.findShortestSeparator("abcdef".getBytes(), "abc".getBytes()));

        // normal case
        assertArrayEquals("abd".getBytes(),
                comparator.findShortestSeparator("abcdef".getBytes(), "abghijk".getBytes()));
        assertArrayEquals("y".getBytes(),
                comparator.findShortestSeparator("xcvmnz".getBytes(), "z".getBytes()));
        assertArrayEquals("abcdef".getBytes(), comparator.findShortestSeparator("abcdef".getBytes(),
                "abdxcvzxcv".getBytes()));

        byte[] tmp1 = "abcdef".getBytes();
        byte[] tmp2 = "xcvzxc".getBytes();
        byte[] target = new byte[tmp1.length + tmp2.length + 1];
        System.arraycopy(tmp1, 0, target, 0, tmp1.length);
        target[tmp1.length] = Byte.MAX_VALUE;
        System.arraycopy(tmp2, 0, target, tmp1.length + 1, tmp2.length);
        assertArrayEquals(target, comparator.findShortestSeparator(target, "abcdefg".getBytes()));
    }
}