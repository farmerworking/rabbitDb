package com.farmerworking.db.rabbitDb.sstable;

import com.farmerworking.db.rabbitDb.Coding;
import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 18/10/15.
 */
public class BlockHandleTest {
    @Test
    public void testSimple() throws Exception {
        BlockHandle blockHandle = new BlockHandle(111l, 100l);

        StringBuilder stringBuilder = new StringBuilder();
        blockHandle.encodeTo(stringBuilder);

        BlockHandle another = new BlockHandle();
        assertEquals(another.getOffset(), 0l);
        assertEquals(another.getSize(), 0l);

        Pair<Status, Integer> pair = another.decodeFrom(new Slice(stringBuilder.toString()));
        assertTrue(pair.getLeft().isOk());
        assertEquals(another.getOffset(), 111l);
        assertEquals(another.getSize(), 100l);
        assertEquals(pair.getRight().intValue(), Coding.detectVariantLength(111l) + Coding.detectVariantLength(100l));
    }
}