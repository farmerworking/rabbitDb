package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 18/10/15.
 */
public class BlockHandleTest {
    @Test
    public void testSimple() throws Exception {
        BlockHandle blockHandle = new BlockHandle(111, 100);

        String encode = blockHandle.encode();

        BlockHandle another = new BlockHandle();
        assertEquals(another.getOffset(), 0l);
        assertEquals(another.getSize(), 0l);

        Pair<Status, Integer> pair = another.decodeFrom(encode);
        assertTrue(pair.getLeft().isOk());
        assertEquals(another.getOffset(), 111l);
        assertEquals(another.getSize(), 100l);
        assertEquals(pair.getRight().intValue(), Coding.detectVariantLength(111l) + Coding.detectVariantLength(100l));
    }
}