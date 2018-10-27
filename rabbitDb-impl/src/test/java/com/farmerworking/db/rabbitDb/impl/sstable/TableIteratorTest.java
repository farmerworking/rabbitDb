package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.ReadOptions;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.impl.EmptyIterator;
import com.farmerworking.db.rabbitDb.impl.ErrorIterator;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.utils.StringSink;
import com.farmerworking.db.rabbitDb.impl.utils.StringSource;
import org.apache.commons.lang3.tuple.Pair;
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

    @Test
    public void testDataBlockChecksumMissmatch1() {
        Pair<Status, Table> pair = prepareDataBlockChecksumMissmatchCase();

        ReadOptions readOptions = new ReadOptions();
        readOptions.verifyChecksums(false);
        TableIterator iter = pair.getRight().iterator(readOptions);
        assertFalse(iter.isValid());

        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("a", iter.key().toString());
        assertEquals("b", iter.value().toString());

        iter.next();
        assertTrue(iter.isValid());
        assertEquals("b", iter.key().toString());
        assertEquals("c", iter.value().toString());

        iter.next();
        assertFalse(iter.isValid());
    }

    @Test
    public void testDataBlockChecksumMissmatch2() {
        Pair<Status, Table> pair = prepareDataBlockChecksumMissmatchCase();

        ReadOptions readOptions = new ReadOptions();
        readOptions.verifyChecksums(true);
        TableIterator iter = pair.getRight().iterator(readOptions);
        assertFalse(iter.isValid());

        iter.seekToFirst();
        assertTrue(iter.isValid());
        assertEquals("a", iter.key().toString());
        assertEquals("b", iter.value().toString());

        iter.next();
        assertFalse(iter.isValid());
        assertTrue(iter.getStatus().isCorruption());
        assertEquals("block checksum mismatch", iter.getStatus().getMessage());
    }

    private Pair<Status, Table> prepareDataBlockChecksumMissmatchCase() {
        StringSink target = new StringSink();
        TableBuilder builder = new TableBuilder(options, target);
        builder.add(new Slice("a"), new Slice("b"));
        builder.flush();
        builder.add(new Slice("b"), new Slice("c"));
        Status status = builder.finish();
        assertTrue(status.isOk());

        // modify data block checksum, 33 is the last char of second block's checksum
        char[] tableContent = target.getContent().toCharArray();
        tableContent[33] = (char)((int)tableContent[33] + 1);

        Pair<Status, Table> pair = Table.open(options, new StringSource(new String(tableContent)), tableContent.length);
        assertTrue(pair.getLeft().isOk());
        return pair;
    }
}