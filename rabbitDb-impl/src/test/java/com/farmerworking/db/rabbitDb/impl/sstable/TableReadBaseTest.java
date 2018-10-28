package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.CompressionType;
import com.farmerworking.db.rabbitDb.api.ReadOptions;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.utils.ErrorSnappy;
import com.farmerworking.db.rabbitDb.impl.utils.ReadEmptyRandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.utils.SnappyWrapper;
import com.farmerworking.db.rabbitDb.impl.utils.StringSource;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

public class TableReadBaseTest {

    @Test
    public void testPartialRead() {
        Pair<Status, Block> pair = TableReadBase.readBlock(new ReadEmptyRandomAccessFile(), new ReadOptions(), new BlockHandle(100, 200), new SnappyWrapper());
        assertTrue(pair.getLeft().isNotOk());
        assertTrue(pair.getLeft().isCorruption());
        assertEquals("truncated block read", pair.getLeft().getMessage());
    }

    @Test
    public void testUncompressError() {
        Pair<Status, Block> pair = TableReadBase.readBlock(
                new StringSource("abcd" + (char)CompressionType.SNAPPY.persistentId() + "sdkjfaldf"),
                new ReadOptions(),
                new BlockHandle(0, 4), new ErrorSnappy());
        assertTrue(pair.getLeft().isNotOk());
        assertTrue(pair.getLeft().isCorruption());
        assertEquals("corrupted compressed block contents", pair.getLeft().getMessage());
    }
}