package com.farmerworking.db.rabbitDb.impl.memtable;

import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by John on 18/7/16.
 */
public class InternalEntryComparatorTest {

    @Test
    public void testCompare() {
        InternalEntryComparator comparator = new InternalEntryComparator(ByteWiseComparator.getInstance());

        InternalEntry key1 = new InternalEntry(new InternalKey("a", 0, ValueType.VALUE),
                null);
        InternalEntry key2 = new InternalEntry(new InternalKey("b", 0, ValueType.VALUE),
                null);
        assertEquals(-1, comparator.compare(key1, key2));

        InternalEntry key3 = new InternalEntry(new InternalKey("a", 1000, ValueType.VALUE),
                null);
        assertEquals(1, comparator.compare(key1, key3));

        InternalEntry key4 = new InternalEntry(new InternalKey("a", 0, ValueType.DELETE),
                null);
        assertEquals(0, comparator.compare(key1, key4));
    }
}