package com.farmerworking.db.rabbitDb.impl;

import com.farmerworking.db.rabbitDb.api.Slice;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by John on 18/7/16.
 */
public class SliceTest {

    @Test
    public void testEmpty() {
        Slice slice = new Slice();

        assertEquals(0, slice.getSize());
        assertTrue(slice.isEmpty());
    }

    @Test
    public void testGet() {
        Slice slice = new Slice(new char[]{65, 66, 67});
        assertEquals((char) 66, slice.get(1));
    }

    @Test
    public void testClear() {
        Slice slice = new Slice(new char[]{65, 66, 67});
        assertFalse(slice.isEmpty());
        slice.clear();
        assertTrue(slice.isEmpty());
    }

    @Test
    public void testCompare() {
        Slice slice1 = new Slice(new char[]{65, 66, 67});
        Slice slice2 = new Slice(new char[]{65, 66, 67});
        Slice slice3 = new Slice(new char[]{65, 65, 67});
        Slice slice4 = new Slice(new char[]{65, 67, 67});

        assertEquals(0, slice1.compareTo(slice2));
        assertTrue(slice1.compareTo(slice3) > 0);
        assertTrue(slice1.compareTo(slice4) < 0);
    }

    @Test
    public void testStartWith() {
        Slice slice1 = new Slice(new char[]{65, 66, 67});
        Slice slice2 = new Slice(new char[]{65, 66, 67});
        Slice slice3 = new Slice(new char[]{63, 66, 67});

        assertTrue(slice1.startsWith(slice2));
        assertFalse(slice1.startsWith(slice3));
    }

    @Test
    public void testRemovePrefix() {
        Slice slice1 = new Slice(new char[]{65, 66, 67});
        Slice slice2 = new Slice(new char[]{65, 66, 67});
        slice1.removePrefix(1);

        assertTrue(Arrays.equals(new char[]{66, 67}, slice1.getData()));
        assertEquals(2, slice1.getSize());
        assertEquals((char) 66, slice1.get(0));
        assertTrue(slice1.compareTo(slice2) > 0);
        assertTrue(slice1.startsWith(new Slice(new char[]{66})));
    }

    @Test
    public void testToString() {
        Slice slice = new Slice("what");

        assertEquals("what", slice.toString());
        assertEquals("", (new Slice()).toString());
    }

    @Test
    public void testEquals() {
        Slice slice1 = new Slice(new char[]{65, 66, 67, 68, 69});
        Slice slice2 = new Slice(new char[]{68, 69});

        slice1.removePrefix(3);
        assertEquals(slice1, slice2);
    }
}