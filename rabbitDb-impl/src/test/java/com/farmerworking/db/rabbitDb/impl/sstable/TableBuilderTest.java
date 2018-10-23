package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.utils.StringSink;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 18/10/24.
 */
public class TableBuilderTest {
    TableBuilder builder;
    Options options;
    StringSink file;

    @Before
    public void setUp() throws Exception {
        options = new Options();
        options.comparator(ByteWiseComparator.getInstance());
        file = new StringSink();
        builder = new TableBuilder(options, file);
    }

    @Test
    public void testAbandon() throws Exception {
        builder.abandon();
    }

    @Test(expected = AssertionError.class)
    public void testAbandon1() throws Exception {
        builder.abandon();
        builder.add(new Slice(""), new Slice(""));
    }

    @Test(expected = AssertionError.class)
    public void testAbandon2() throws Exception {
        builder.abandon();
        builder.abandon();
    }

    @Test(expected = AssertionError.class)
    public void testAbandon3() throws Exception {
        builder.abandon();
        builder.finish();
    }

    @Test(expected = AssertionError.class)
    public void testAbandon4() throws Exception {
        builder.abandon();
        builder.flush();
    }

    @Test
    public void testNumEntries() throws Exception {
        assertEquals(0, builder.numEntries());
    }

    @Test
    public void testNumEntries1() throws Exception {
        builder.add(new Slice(""), new Slice(""));
        builder.add(new Slice("a"), new Slice(""));
        assertEquals(2, builder.numEntries());
    }

    @Test
    public void testFileSize() throws Exception {
        assertEquals(0, builder.fileSize());
        builder.add(new Slice(""), new Slice(""));
        builder.add(new Slice("a"), new Slice(""));
        assertEquals(0, builder.fileSize());
        builder.flush();
        long flushFileSize = builder.fileSize();
        assertTrue(flushFileSize > 0);
        builder.finish();
        long finishFileSize = builder.fileSize();
        assertTrue(finishFileSize > flushFileSize);
        assertEquals(file.getContent().length(), finishFileSize);
    }

    @Test
    public void testFlushSkipIfEmpty() throws Exception {
        builder.add(new Slice(""), new Slice(""));
        builder.add(new Slice("a"), new Slice(""));
        builder.flush();
        long flushFileSize = builder.fileSize();
        builder.flush();
        long flushFileSize2 = builder.fileSize();
        assertEquals(flushFileSize, flushFileSize2);
    }

    @Test
    public void testFinish() throws Exception {
        builder.finish();
    }

    @Test(expected = AssertionError.class)
    public void testFinish1() throws Exception {
        builder.finish();
        builder.add(new Slice(""), new Slice(""));
    }

    @Test(expected = AssertionError.class)
    public void testFinish2() throws Exception {
        builder.finish();
        builder.finish();
    }

    @Test(expected = AssertionError.class)
    public void testFinish3() throws Exception {
        builder.finish();
        builder.abandon();
    }

    @Test(expected = AssertionError.class)
    public void testFinish4() throws Exception {
        builder.finish();
        builder.flush();
    }

    @Test(expected = AssertionError.class)
    public void testAdd() throws Exception {
        builder.add(new Slice("b"), new Slice(""));
        builder.add(new Slice("a"), new Slice(""));
    }

    @Test
    public void testBadStatusAdd() throws Exception {
        builder.add(new Slice(""), new Slice(""));
        assertEquals(1, builder.numEntries());
        builder.setStatus(Status.corruption("bad status"));
        assertTrue(builder.status().isNotOk());

        builder.add(new Slice("a"), new Slice(""));
        assertEquals(1, builder.numEntries());
    }

    @Test
    public void testBadStatusFlush() throws Exception {
        builder.add(new Slice(""), new Slice(""));
        builder.flush();
        int originLength = file.getContent().length();
        assertEquals(file.getContent().length(), builder.fileSize());
        assertEquals(1, builder.numEntries());

        builder.add(new Slice("a"), new Slice(""));
        builder.setStatus(Status.corruption("bad status"));
        assertTrue(builder.status().isNotOk());
        builder.flush();

        assertEquals(2, builder.numEntries());
        assertEquals(file.getContent().length(), builder.fileSize());
        assertEquals(originLength, builder.fileSize());
    }

    @Test
    public void testBadStatusFinish() throws Exception {
        builder.add(new Slice(""), new Slice(""));
        builder.add(new Slice("a"), new Slice(""));
        builder.setStatus(Status.corruption("bad status"));
        Status status = builder.finish();
        assertTrue(status.isNotOk());
        assertEquals(0, file.getContent().length());
    }
}