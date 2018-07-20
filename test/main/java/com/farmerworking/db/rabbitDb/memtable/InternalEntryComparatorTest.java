package com.farmerworking.db.rabbitDb.memtable;

import static org.junit.Assert.assertEquals;

import com.farmerworking.db.rabbitDb.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.Slice;
import org.junit.Test;

/**
 * Created by John on 18/7/16.
 */
public class InternalEntryComparatorTest {

  @Test
  public void testCompare() throws Exception {
    InternalEntryComparator comparator = new InternalEntryComparator(ByteWiseComparator.getInstance());

    InternalEntry key1 = new InternalEntry(new InternalKey(new Slice("a".getBytes()), 0, ValueType.VALUE), null);
    InternalEntry key2 = new InternalEntry(new InternalKey(new Slice("b".getBytes()), 0, ValueType.VALUE), null);
    assertEquals(-1, comparator.compare(key1, key2));

    InternalEntry key3 = new InternalEntry(new InternalKey(new Slice("a".getBytes()), 1000, ValueType.VALUE), null);
    assertEquals(1, comparator.compare(key1, key3));

    InternalEntry key4 = new InternalEntry(new InternalKey(new Slice("a".getBytes()), 0, ValueType.DELETE), null);
    assertEquals(0, comparator.compare(key1, key4));
  }
}