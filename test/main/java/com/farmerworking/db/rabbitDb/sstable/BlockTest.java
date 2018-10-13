package com.farmerworking.db.rabbitDb.sstable;

import com.farmerworking.db.rabbitDb.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.Coding;
import com.farmerworking.db.rabbitDb.DBIterator;
import com.farmerworking.db.rabbitDb.Slice;
import org.iq80.leveldb.Options;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockTest {
    private Options options;

    @Before
    public void setUp() throws Exception {
        options = new Options();
        options.comparator(ByteWiseComparator.getInstance());
    }

    @Test
    public void testReset() {
        BlockBuilder builder = new BlockBuilder(options);

        assertTrue(builder.isEmpty());
        builder.add(new Slice("tmp"), new Slice("value"));
        assertFalse(builder.isEmpty());
        builder.reset();
        assertTrue(builder.isEmpty());
    }

    @Test(expected = AssertionError.class)
    public void testAddAfterFinish() {
        BlockBuilder builder = new BlockBuilder(options);
        builder.finish();
        builder.add(new Slice(""), new Slice(""));
    }

    @Test(expected = AssertionError.class)
    public void testAddErrorKey() {
        BlockBuilder builder = new BlockBuilder(options);
        builder.add(new Slice("2"), new Slice("two"));
        builder.add(new Slice("1"), new Slice("two"));
    }

    @Test
    public void testMalformBlock() {
        Block block = new Block(Slice.EMPTY_SLICE);
        DBIterator<Slice, Slice> iter = block.iterator(options.comparator());
        assertTrue(iter.getStatus().isCorruption());
        assertFalse(iter.isValid());
    }

    @Test
    public void testMalformBlock2() {
        char[] buffer = new char[4];
        Coding.encodeFixed32(buffer, 0, 1);
        Block block = new Block(new Slice(buffer));
        DBIterator<Slice, Slice> iter = block.iterator(options.comparator());
        assertTrue(iter.getStatus().isCorruption());
        assertFalse(iter.isValid());
    }

    @Test
    public void testZeroRestarts() {
        char[] buffer = new char[4];
        Coding.encodeFixed32(buffer, 0, 0);
        Block block = new Block(new Slice(buffer));
        DBIterator<Slice, Slice> iter = block.iterator(options.comparator());
        assertTrue(iter.getStatus().isOk());
        assertFalse(iter.isValid());

        iter.seekToFirst();
        assertFalse(iter.isValid());
    }

    @Test
    public void testIterNext() throws Exception {
        BlockBuilder builder = new BlockBuilder(options);
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add(new Slice("test" + String.valueOf((char) i)), new Slice(String.valueOf(i)));
        }

        Slice content = builder.finish();
        Block block = new Block(content);
        DBIterator<Slice, Slice> iterator = block.iterator(options.comparator());

        iterator.seekToFirst();
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            assertTrue(iterator.isValid());
            assertEquals("test" + String.valueOf((char) i), iterator.key().toString());
            assertEquals(String.valueOf(i), iterator.value().toString());
            iterator.next();
        }
    }

    @Test
    public void testIterPrev() throws Exception {
        BlockBuilder builder = new BlockBuilder(options);
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add(new Slice("test" + String.valueOf((char) i)), new Slice(String.valueOf(i)));
        }

        Slice content = builder.finish();
        Block block = new Block(content);
        DBIterator<Slice, Slice> iterator = block.iterator(options.comparator());

        iterator.seekToLast();
        for (int i = options.blockRestartInterval() * 6 - 1; i >= 0; i--) {
            assertTrue(iterator.isValid());
            assertEquals("test" + String.valueOf((char) i), iterator.key().toString());
            assertEquals(String.valueOf(i), iterator.value().toString());
            iterator.prev();
        }
    }

    @Test
    public void testIterSeekNext() throws Exception {
        BlockBuilder builder = new BlockBuilder(options);
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add(new Slice("test" + String.valueOf((char) i)), new Slice(String.valueOf(i)));
        }

        Slice content = builder.finish();
        Block block = new Block(content);
        DBIterator<Slice, Slice> iterator = block.iterator(options.comparator());

        iterator.seek(new Slice("test" + String.valueOf((char) 100)));
        for (int i = 100; i < options.blockRestartInterval() * 6; i++) {
            assertTrue(iterator.isValid());
            assertEquals("test" + String.valueOf((char) i), iterator.key().toString());
            assertEquals(String.valueOf(i), iterator.value().toString());
            iterator.next();
        }
    }

    @Test
    public void testIterSeekPrev() throws Exception {
        BlockBuilder builder = new BlockBuilder(options);
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add(new Slice("test" + String.valueOf((char) i)), new Slice(String.valueOf(i)));
        }

        Slice content = builder.finish();
        Block block = new Block(content);
        DBIterator<Slice, Slice> iterator = block.iterator(options.comparator());

        iterator.seek(new Slice("test" + String.valueOf((char) 90)));
        for (int i = 90; i >= 0; i--) {
            assertTrue(iterator.isValid());
            assertEquals("test" + String.valueOf((char) i), iterator.key().toString());
            assertEquals(String.valueOf(i), iterator.value().toString());
            iterator.prev();
        }
    }

    @Test
    public void testNextPrev() throws Exception {
        BlockBuilder builder = new BlockBuilder(options);
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add(new Slice("test" + String.valueOf((char) i)), new Slice(String.valueOf(i)));
        }

        Slice content = builder.finish();
        Block block = new Block(content);
        DBIterator<Slice, Slice> iterator = block.iterator(options.comparator());

        iterator.seek(new Slice("test" + String.valueOf((char) 90)));

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 90), iterator.key().toString());
        assertEquals("90", iterator.value().toString());
        iterator.next();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 91), iterator.key().toString());
        assertEquals("91", iterator.value().toString());
        iterator.next();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 92), iterator.key().toString());
        assertEquals("92", iterator.value().toString());
        iterator.prev();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 91), iterator.key().toString());
        assertEquals("91", iterator.value().toString());
        iterator.prev();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 90), iterator.key().toString());
        assertEquals("90", iterator.value().toString());
        iterator.prev();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 89), iterator.key().toString());
        assertEquals("89", iterator.value().toString());
    }

    @Test
    public void testEmpty() throws Exception {
        BlockBuilder builder = new BlockBuilder(options);
        Slice content = builder.finish();
        Block block = new Block(content);
        DBIterator<Slice, Slice> iterator = block.iterator(options.comparator());
        iterator.seekToFirst();
        assertFalse(iterator.isValid());
        assertTrue(iterator.getStatus().isOk());
    }

    @Test
    public void testSizeEstimate() {
        BlockBuilder builder = new BlockBuilder(options);
        int emptySize = builder.currentSizeEstimate();

        builder.add(new Slice("one"), new Slice("two"));
        int oneSize = builder.currentSizeEstimate();
        assertTrue(oneSize > emptySize);

        builder.add(new Slice("two"), new Slice("three"));
        int twoSize = builder.currentSizeEstimate();
        assertTrue(twoSize > oneSize);

        builder.add(new Slice("txree"), new Slice("four"));
        int threeSize = builder.currentSizeEstimate();
        assertTrue(threeSize > twoSize);

        builder.finish();
        int finishSize = builder.currentSizeEstimate();
        assertEquals(threeSize, finishSize);
    }
}