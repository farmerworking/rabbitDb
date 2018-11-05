package com.farmerworking.db.rabbitDb.impl;

import com.farmerworking.db.rabbitDb.api.*;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.Map.Entry;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by John on 18/7/12.
 */
public class DbImplTest {

    private DbImpl db;

    @Before
    public void setUp() {
        db = new DbImpl(new Options());
    }

    @Test
    public void testEmpty() {
        assertNull(getString("one"));

        DBIteratorImpl iter = db.iterator();
        assertFalse(iter.isValid());
    }

    @Test
    public void testSimpleReadWrite() {
        put("one", "1");
        put("two", "2");
        put("two", "22");
        put("three", "3");
        delete("three");

        assertEquals("1", getString("one"));
        assertEquals("22", getString("two"));
        assertNull(getString("three"));

        DBIteratorImpl iter = db.iterator();
        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("one", iter.key());
        assertEquals("1", iter.value());

        iter.next();
        assertTrue(iter.isValid());
        assertEquals("two", iter.key());
        assertEquals("22", iter.value());

        iter.next();
        assertFalse(iter.isValid());
    }

    @Test
    public void testManyReadWrite() {
        int N = 1000;
        int R = 100;
        SortedMap<String, String> state = new TreeMap<>();

        Random random = new Random();
        for (int i = 0; i < N; i++) {
            Long key = (long) (Math.abs(random.nextInt()) % R);

            if ((random.nextInt() % 4) == 0) {
                state.remove(String.valueOf(key));
                delete(String.valueOf(key));
            } else {
                state.put(String.valueOf(key),
                        String.valueOf(Math.abs(random.nextInt())));
                put(String.valueOf(key), new String(state.get(String.valueOf(key)).toCharArray()));
            }
        }

        for (long i = 0; i < R; i++) {
            if (state.containsKey(String.valueOf(i))) {
                assertEquals(new String(state.get(String.valueOf(i)).toCharArray()),
                        getString(String.valueOf(i)));
            } else {
                assertNull(getString(String.valueOf(i)));
            }
        }

        // Simple skipListIterator tests
        DBIteratorImpl iterator = db.iterator();
        assertFalse(iterator.isValid());

        iterator.seekToFirst();
        assertTrue(iterator.isValid());
        assertEquals(state.firstKey(), iterator.key());
        assertEquals(((TreeMap<String, String>) state).firstEntry().getValue(),
                iterator.value());

        iterator.seekToLast();
        assertTrue(iterator.isValid());
        assertEquals(state.lastKey(), iterator.key());
        assertEquals(((TreeMap<String, String>) state).lastEntry().getValue(), iterator.value());

        // Forward iteration test
        for (long i = 0; i < R; i++) {
            DBIteratorImpl iter = db.iterator();
            iter.seek(String.valueOf(i));

            // Compare against model skipListIterator
            Iterator<Entry<String, String>> iterator1 = (state
                    .tailMap(String.valueOf(i))).entrySet().iterator();
            for (int j = 0; j < 3; j++) {
                if (iterator1.hasNext()) {
                    assertTrue(iter.isValid());
                    Entry<String, String> entry = iterator1.next();
                    assertEquals(entry.getKey(), iter.key());
                    assertEquals(entry.getValue(), iter.value());
                    iter.next();
                } else {
                    assertFalse(iter.isValid());
                    break;
                }
            }
        }

        // Backward iteration test
        DBIteratorImpl iter = db.iterator();
        iter.seekToLast();

        // Compare against model skipListIterator
        SortedMap<String, String> reverseState = new TreeMap<>(Collections.reverseOrder());
        reverseState.putAll(state);
        Iterator<Entry<String, String>> iterator1 = reverseState.entrySet().iterator();
        while (iterator1.hasNext()) {
            assertTrue(iter.isValid());
            Entry<String, String> entry = iterator1.next();
            assertEquals(entry.getKey(), iter.key());
            assertEquals(entry.getValue(), iter.value());
            iter.prev();
        }
        assertFalse(iter.isValid());
    }


    @Test(expected = DBException.class)
    public void testMemoryExceed() {
        Options options = new Options();
        options.writeBufferSize(0);
        db = new DbImpl(options);

        put("one", "1");
    }

    @Test
    public void testSnapshot() {
        put("one", "1");
        put("two", "2");
        put("three", "3");

        // snapshot before latest update
        DBIteratorImpl iter = db.iterator();

        put("two", "22");
        delete("three");

        assertEquals("1", getString("one"));
        assertEquals("22", getString("two"));
        assertNull(getString("three"));

        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("one", iter.key());
        assertEquals("1", iter.value());

        iter.next();
        assertTrue(iter.isValid());
        assertEquals("three", iter.key());
        assertEquals("3", iter.value());

        iter.next();
        assertTrue(iter.isValid());
        assertEquals("two", iter.key());
        assertEquals("2", iter.value());

        iter.next();
        assertFalse(iter.isValid());

        // snapshot after latest update
        iter = db.iterator();
        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("one", iter.key());
        assertEquals("1", iter.value());

        iter.next();
        assertTrue(iter.isValid());
        assertEquals("two", iter.key());
        assertEquals("22", iter.value());

        iter.next();
        assertFalse(iter.isValid());
    }

    @Test
    public void testSnapshot2() {
        Snapshot snapshot = db.getSnapshot();

        Snapshot snapshot1 = put("1", "1");
        Snapshot snapshot2 = put("1", "111");
        Snapshot snapshot3 = put("2", "2");
        Snapshot snapshot4 = delete("2");

        assertEquals("111", getString("1"));
        assertNull(getString("2"));
        DBIteratorImpl iter = iterator(null);
        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("1", iter.key());
        assertEquals("111", iter.value());
        iter.next();
        assertFalse(iter.isValid());

        assertNull(getString("1", snapshot));
        assertNull(getString("2", snapshot));
        iter = iterator(snapshot);
        iter.seekToFirst();
        assertFalse(iter.isValid());

        assertEquals("1", getString("1", snapshot1));
        assertNull(getString("2", snapshot1));
        iter = iterator(snapshot1);
        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("1", iter.key());
        assertEquals("1", iter.value());
        iter.next();
        assertFalse(iter.isValid());

        assertEquals("111", getString("1", snapshot2));
        assertNull(getString("2", snapshot2));
        iter = iterator(snapshot2);
        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("1", iter.key());
        assertEquals("111", iter.value());
        iter.next();
        assertFalse(iter.isValid());

        assertEquals("111", getString("1", snapshot3));
        assertEquals("2", getString("2", snapshot3));
        iter = iterator(snapshot3);
        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("1", iter.key());
        assertEquals("111", iter.value());
        iter.next();
        assertTrue(iter.isValid());
        assertEquals("2", iter.key());
        assertEquals("2", iter.value());
        iter.next();
        assertFalse(iter.isValid());

        assertEquals("111", getString("1", snapshot4));
        assertNull(getString("2", snapshot4));
        iter = iterator(snapshot4);
        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("1", iter.key());
        assertEquals("111", iter.value());
        iter.next();
        assertFalse(iter.isValid());
    }

    private String getString(String key) {
        return getString(key, null);
    }

    private String getString(String key, Snapshot snapshot) {
        String value;
        if (snapshot == null) {
            value = db.get(key);
        } else {
            ReadOptions options = new ReadOptions();
            options.snapshot(snapshot);
            value = db.get(key, options);
        }

        return value == null ? null : value;
    }

    private Snapshot put(String key, String value) {
        return db.put(key, value);
    }

    private Snapshot delete(String key) {
        return db.delete(key);
    }

    private DBIteratorImpl iterator(Snapshot snapshot) {
        if (snapshot == null) {
            return db.iterator();
        } else {
            ReadOptions options = new ReadOptions();
            options.snapshot(snapshot);
            return db.iterator(options);
        }
    }
}