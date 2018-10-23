package com.farmerworking.db.rabbitDb.impl;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by John on 18/7/11.
 */
public class ByteWiseComparatorTest {
    @Test
    public void testCompare0() throws Exception {
        String a = "" + (char) 126;
        String b = "" + (char) 127;
        String c = "" + (char) 128;

        DBComparator comparator = ByteWiseComparator.getInstance();
        assertTrue(comparator.compare(a, b) < 0);
        assertTrue(comparator.compare(b, c) < 0);
    }

    @Test
    public void testCompare() throws Exception {
        DBComparator comparator = ByteWiseComparator.getInstance();
        assertEquals(0, comparator.compare("b", "b"));
        assertEquals(-1, comparator.compare("b", "c"));
        assertEquals(1, comparator.compare("b", "a"));
    }

    @Test
    public void testFindShortSuccessor() throws Exception {
        DBComparator comparator = ByteWiseComparator.getInstance();
        assertEquals("b", comparator.findShortSuccessor("abcdef"));

        assertArrayEquals(new char[]{Character.MAX_VALUE},
                comparator.findShortSuccessor(new char[]{Character.MAX_VALUE}));
        assertArrayEquals(new char[]{Character.MAX_VALUE, Character.MAX_VALUE, Character.MAX_VALUE, "b".toCharArray()[0]},
                comparator.findShortSuccessor(
                        new char[]{Character.MAX_VALUE, Character.MAX_VALUE, Character.MAX_VALUE, "a".toCharArray()[0],
                                "b".toCharArray()[0]}));
    }

    @Test
    public void testFindShortestSeparator() throws Exception {
        DBComparator comparator = ByteWiseComparator.getInstance();

        // prefix
        assertEquals("abcdef", comparator.findShortestSeparator("abcdef", "abcdefghijk"));
        assertEquals("abcdef", comparator.findShortestSeparator("abcdef", "abc"));

        // normal case
        assertEquals("abd", comparator.findShortestSeparator("abcdef", "abghijk"));
        assertEquals("y", comparator.findShortestSeparator("xcvmnz", "z"));
        assertEquals("abcdef", comparator.findShortestSeparator("abcdef", "abdxcvzxcv"));

        char[] tmp1 = "abcdef".toCharArray();
        char[] tmp2 = "xcvzxc".toCharArray();
        char[] target = new char[tmp1.length + tmp2.length + 1];
        System.arraycopy(tmp1, 0, target, 0, tmp1.length);
        target[tmp1.length] = Byte.MAX_VALUE;
        System.arraycopy(tmp2, 0, target, tmp1.length + 1, tmp2.length);
        assertEquals(target, comparator.findShortestSeparator(target, "abcdefg".toCharArray()));
    }
}