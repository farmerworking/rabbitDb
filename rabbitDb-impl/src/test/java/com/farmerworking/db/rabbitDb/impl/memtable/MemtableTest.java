package com.farmerworking.db.rabbitDb.impl.memtable;

import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.api.Slice;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 18/7/12.
 */
public class MemtableTest {

    private Memtable memtable;

    @Before
    public void setUp() {
        memtable = new Memtable(ByteWiseComparator.getInstance());
    }

    @Test
    public void testEmpty() {
        assertNull(get("one"));
        MemtableIterator iter = memtable.iterator();
        iter.seekToFirst();
        assertFalse(iter.isValid());
    }

    @Test
    public void testWriteRead() {
        long sequence = 100;
        put(sequence++, "one", "1");
        put(sequence++, "two", "2");
        put(sequence++, "three", "3");
        put(sequence++, "four", "4");
        put(sequence++, "three", "33");
        put(sequence, "four", "44");

        assertEquals("1", get("one"));
        assertEquals("2", get("two"));
        assertEquals("33", get("three"));
        assertEquals("44", get("four"));

        assertNull(get(1, "one"));
        assertNull(get(1, "two"));
        assertNull(get(1, "three"));
        assertNull(get(1, "four"));
    }

    @Test
    public void testDelete() {
        long sequence = 100;
        delete(sequence++, "one");
        assertNull(get("one"));

        put(sequence++, "one", "1");
        assertNull(get(1, "one"));
        assertEquals("1", get("one"));

        delete(sequence, "one");
        assertNull(get("one"));
    }

    @Test
    public void testIterator() {
        MemtableIterator iter = memtable.iterator();
        iter.seekToFirst();
        assertFalse(iter.isValid());

        long sequence = 100;
        put(sequence++, "one", "1");
        put(sequence++, "two", "2");
        put(sequence++, "three", "3");
        put(sequence++, "four", "4");
        put(sequence++, "three", "33");
        put(sequence++, "four", "44");
        delete(sequence, "one");

        iter = memtable.iterator();

        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertVariousAndNext(iter, "four", 105, ValueType.VALUE, "44");
        assertVariousAndNext(iter, "four", 103, ValueType.VALUE, "4");
        assertVariousAndNext(iter, "one", 106, ValueType.DELETE, null);
        assertVariousAndNext(iter, "one", 100, ValueType.VALUE, "1");
        assertVariousAndNext(iter, "three", 104, ValueType.VALUE, "33");
        assertVariousAndNext(iter, "three", 102, ValueType.VALUE, "3");
        assertVariousAndNext(iter, "two", 101, ValueType.VALUE, "2");

        assertFalse(iter.isValid());

        iter.seek(new InternalKey(new Slice("four"), 100, null));
        assertTrue(iter.isValid());
        assertVariousAndNext(iter, "one", 106, ValueType.DELETE, null);

        iter.seek(new InternalKey(new Slice("one"), 110, null));
        assertTrue(iter.isValid());
        assertVariousAndNext(iter, "one", 106, ValueType.DELETE, null);

        iter.seek(new InternalKey(new Slice("x"), 1000, null));
        assertFalse(iter.isValid());
    }

    @Test
    public void testSize() {
        long initSize = memtable.approximateMemoryUsage();

        long sequence = 100;
        put(sequence++, "one", "1");
        long firstSize = memtable.approximateMemoryUsage();
        assertTrue(firstSize > initSize);

        put(sequence, "two", "2");
        long secondSize = memtable.approximateMemoryUsage();
        assertTrue(secondSize > firstSize);
    }

    private String get(String key) {
        return get(Long.MAX_VALUE, key);
    }

    private String get(long sequence, String key) {
        Slice value = memtable.get(new InternalKey(new Slice(key), sequence, null));
        return value == null ? null : value.toString();
    }

    private void put(long sequence, String key, String value) {
        memtable.add(new InternalKey(new Slice(key), sequence, ValueType.VALUE), new Slice(value));
    }

    private void delete(long sequence, String key) {
        memtable.add(new InternalKey(new Slice(key), sequence, ValueType.DELETE), new Slice());
    }

    private void assertVariousAndNext(MemtableIterator iterator, String userKey, long sequence,
                                      ValueType type, String value) {
        InternalKey internalKey = iterator.key();
        Slice returnValue = iterator.value();

        assertEquals(userKey, internalKey.getUserKey().toString());
        assertEquals(sequence, internalKey.getSequence());
        assertEquals(type, internalKey.getValueType());

        if (type == ValueType.VALUE) {
            assertEquals(value, returnValue.toString());
        }

        iterator.next();
    }
}