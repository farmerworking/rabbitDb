package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;
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
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());

        assertTrue(builder.isEmpty());
        builder.add("tmp", "value");
        assertFalse(builder.isEmpty());
        builder.reset();
        assertTrue(builder.isEmpty());
    }

    @Test(expected = AssertionError.class)
    public void testAddAfterFinish() {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        builder.finish();
        builder.add("", "");
    }

    @Test(expected = AssertionError.class)
    public void testAddErrorKey() {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        builder.add("2", "two");
        builder.add("1", "two");
    }

    @Test
    public void testMalformBlock() {
        Block block = new Block("");
        DBIterator<String, String> iter = block.iterator(options.comparator());
        assertTrue(iter.getStatus().isCorruption());
        assertFalse(iter.isValid());
    }

    @Test
    public void testMalformBlock2() {
        char[] buffer = new char[4];
        Coding.encodeFixed32(buffer, 0, 1);
        Block block = new Block(new String(buffer));
        DBIterator<String, String> iter = block.iterator(options.comparator());
        assertTrue(iter.getStatus().isCorruption());
        assertFalse(iter.isValid());
    }

    @Test
    public void testZeroRestarts() {
        char[] buffer = new char[4];
        Coding.encodeFixed32(buffer, 0, 0);
        Block block = new Block(new String(buffer));
        DBIterator<String, String> iter = block.iterator(options.comparator());
        assertTrue(iter.getStatus().isOk());
        assertFalse(iter.isValid());

        iter.seekToFirst();
        assertFalse(iter.isValid());
    }

    @Test
    public void testIterNext() throws Exception {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add("test" + String.valueOf((char) i), String.valueOf(i));
        }

        String content = builder.finish();
        Block block = new Block(content);
        DBIterator<String, String> iterator = block.iterator(options.comparator());

        iterator.seekToFirst();
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            assertTrue(iterator.isValid());
            assertEquals("test" + String.valueOf((char) i), iterator.key());
            assertEquals(String.valueOf(i), iterator.value());
            iterator.next();
        }
    }

    @Test
    public void testIterPrev() throws Exception {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add("test" + String.valueOf((char) i), String.valueOf(i));
        }

        String content = builder.finish();
        Block block = new Block(content);
        DBIterator<String, String> iterator = block.iterator(options.comparator());

        iterator.seekToLast();
        for (int i = options.blockRestartInterval() * 6 - 1; i >= 0; i--) {
            assertTrue(iterator.isValid());
            assertEquals("test" + String.valueOf((char) i), iterator.key());
            assertEquals(String.valueOf(i), iterator.value());
            iterator.prev();
        }
    }

    @Test
    public void testIterSeekNext() throws Exception {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add("test" + String.valueOf((char) i), String.valueOf(i));
        }

        String content = builder.finish();
        Block block = new Block(content);
        DBIterator<String, String> iterator = block.iterator(options.comparator());

        iterator.seek("test" + String.valueOf((char) 100));
        for (int i = 100; i < options.blockRestartInterval() * 6; i++) {
            assertTrue(iterator.isValid());
            assertEquals("test" + String.valueOf((char) i), iterator.key());
            assertEquals(String.valueOf(i), iterator.value());
            iterator.next();
        }
    }

    @Test
    public void testIterSeekPrev() throws Exception {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add("test" + String.valueOf((char) i), String.valueOf(i));
        }

        String content = builder.finish();
        Block block = new Block(content);
        DBIterator<String, String> iterator = block.iterator(options.comparator());

        iterator.seek("test" + String.valueOf((char) 90));
        for (int i = 90; i >= 0; i--) {
            assertTrue(iterator.isValid());
            assertEquals("test" + String.valueOf((char) i), iterator.key());
            assertEquals(String.valueOf(i), iterator.value());
            iterator.prev();
        }
    }

    @Test
    public void testNextPrev() throws Exception {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        for (int i = 0; i < options.blockRestartInterval() * 6; i++) {
            builder.add("test" + String.valueOf((char) i), String.valueOf(i));
        }

        String content = builder.finish();
        Block block = new Block(content);
        DBIterator<String, String> iterator = block.iterator(options.comparator());

        iterator.seek("test" + String.valueOf((char) 90));

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 90), iterator.key());
        assertEquals("90", iterator.value());
        iterator.next();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 91), iterator.key());
        assertEquals("91", iterator.value());
        iterator.next();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 92), iterator.key());
        assertEquals("92", iterator.value());
        iterator.prev();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 91), iterator.key());
        assertEquals("91", iterator.value());
        iterator.prev();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 90), iterator.key());
        assertEquals("90", iterator.value());
        iterator.prev();

        assertTrue(iterator.isValid());
        assertEquals("test" + String.valueOf((char) 89), iterator.key());
        assertEquals("89", iterator.value());
    }

    @Test
    public void testEmpty() throws Exception {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        String content = builder.finish();
        Block block = new Block(content);
        DBIterator<String, String> iterator = block.iterator(options.comparator());
        iterator.seekToFirst();
        assertFalse(iterator.isValid());
        assertTrue(iterator.getStatus().isOk());
    }

    @Test
    public void testSizeEstimate() {
        BlockBuilder builder = new BlockBuilder(options.blockRestartInterval(), options.comparator());
        int emptySize = builder.currentSizeEstimate();

        builder.add("one", "two");
        int oneSize = builder.currentSizeEstimate();
        assertTrue(oneSize > emptySize);

        builder.add("two", "three");
        int twoSize = builder.currentSizeEstimate();
        assertTrue(twoSize > oneSize);

        builder.add("txree", "four");
        int threeSize = builder.currentSizeEstimate();
        assertTrue(threeSize > twoSize);

        builder.finish();
        int finishSize = builder.currentSizeEstimate();
        assertEquals(threeSize, finishSize);
    }
}