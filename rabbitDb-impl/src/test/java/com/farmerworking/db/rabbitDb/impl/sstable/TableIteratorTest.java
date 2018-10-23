package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.ReadOptions;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.impl.EmptyIterator;
import com.farmerworking.db.rabbitDb.impl.ErrorIterator;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.utils.StringSource;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 18/10/23.
 */
public class TableIteratorTest {
    private Options options;

    @Before
    public void setUp() throws Exception {
        options = new Options();
        options.comparator(ByteWiseComparator.getInstance());
    }

    @Test
    public void testCorruptionIndexBlock() throws Exception {
        Block block = new Block(new Slice());
        DBIterator<Slice, Slice> iter = block.iterator(options.comparator());
        assertTrue(iter.getStatus().isCorruption());

        TableIterator tableIterator = new TableIterator(iter, new ReadOptions(), new StringSource(""), options.comparator());
        assertTrue(tableIterator.getStatus().isCorruption());
    }

    @Test
    public void testCorruptionDataBlock() throws Exception {
        TableIterator tableIterator = new TableIterator(new EmptyIterator<>(), new ReadOptions(), new StringSource(""), options.comparator());
        assertTrue(tableIterator.getStatus().isOk());

        tableIterator.setDataBlockIter(new ErrorIterator<>(Status.corruption("bad block content")));
        assertTrue(tableIterator.getStatus().isCorruption());

        // error status is kept
        tableIterator.seekToFirst();
        assertTrue(tableIterator.getStatus().isCorruption());
    }
}