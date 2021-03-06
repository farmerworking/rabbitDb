package com.farmerworking.db.rabbitDb.impl.writebatch;

import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.impl.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.impl.memtable.Memtable;
import com.farmerworking.db.rabbitDb.impl.memtable.MemtableIterator;
import com.farmerworking.db.rabbitDb.impl.memtable.ValueType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 18/7/16.
 */
public class WriteBatchImplTest {

    private WriteBatchImpl writeBatch;

    @Before
    public void setUp() {
        writeBatch = new WriteBatchImpl();
    }

    @Test
    public void testEmpty() {
        assertEquals(0, writeBatch.getCount());
        assertNull(writeBatch.getSequence());
    }

    @Test
    public void testClear() {
        setSequence(100L);
        put("one", "two");

        assertNotEquals(0, writeBatch.getCount());
        assertNotNull(writeBatch.getSequence());

        writeBatch.clear();
        assertEquals(0, writeBatch.getCount());
        assertNull(writeBatch.getSequence());
    }

    @Test
    public void testMultiple() {
        put("foo", "bar");
        delete("box");
        put("baz", "boo");
        setSequence(100L);

        assertEquals(100L, writeBatch.getSequence().longValue());
        assertEquals(3, writeBatch.getCount());
        assertEquals("Put(baz, boo)@102" +
                        "Delete(box)@101" +
                        "Put(foo, bar)@100",
                printContents());
    }

    @Test
    public void testApproximateSize() {
        int emptySize = writeBatch.approximateSize();

        put("foo", "bar");
        int oneKeySize = writeBatch.approximateSize();
        assertTrue(emptySize < oneKeySize);

        put("baz", "boo");
        int twoKeysSize = writeBatch.approximateSize();
        assertTrue(oneKeySize < twoKeysSize);

        delete("box");
        int deleteKeysSize = writeBatch.approximateSize();
        assertTrue(twoKeysSize < deleteKeysSize);
    }

    @Test
    public void testAppend() {
        WriteBatchImpl b2 = new WriteBatchImpl();
        setSequence(200L);
        b2.setSequence(300L);
        writeBatch.append(b2);

        assertEquals("", printContents());
        b2.put("a", "va");
        writeBatch.append(b2);
        assertEquals("Put(a, va)@200", printContents());

        b2.clear();
        b2.put("b", "vb");
        writeBatch.append(b2);
        assertEquals("Put(a, va)@200" + "Put(b, vb)@201", printContents());

        b2.delete("foo");
        writeBatch.append(b2);
        assertEquals("Put(a, va)@200" +
                "Put(b, vb)@202" +
                "Put(b, vb)@201" +
                "Delete(foo)@203", printContents());
    }

    private void put(String key, String value) {
        writeBatch.put(key, value);
    }

    private void delete(String key) {
        writeBatch.delete(key);
    }

    private void setSequence(Long sequence) {
        writeBatch.setSequence(sequence);
    }

    private String printContents() {
        Memtable memtable = new Memtable(ByteWiseComparator.getInstance());
        writeBatch.iterate(new WriteBatchMemtableIterateHandler(memtable));

        StringBuilder builder = new StringBuilder();
        MemtableIterator iter = memtable.iterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            InternalKey entry = iter.key();
            if (entry.getValueType() == ValueType.DELETE) {
                builder.append(String.format("Delete(%s)@%d",
                        entry.getUserKey(), entry.getSequence()));
            } else {
                builder.append(String.format("Put(%s, %s)@%d",
                        entry.getUserKey(), iter.value(),
                        entry.getSequence()));
            }
        }

        return builder.toString();
    }
}