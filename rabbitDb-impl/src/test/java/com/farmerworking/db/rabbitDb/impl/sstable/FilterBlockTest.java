package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.FilterPolicy;
import com.farmerworking.db.rabbitDb.impl.utils.TestHashFilter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilterBlockTest {

    private FilterPolicy policy;

    @Before
    public void setUp() throws Exception {
        this.policy = new TestHashFilter();
    }

    @Test
    public void testEmptyBuilder() {
        FilterBlockBuilder builder = new FilterBlockBuilder(policy);
        String block = builder.finish();
        assertEquals("\\x00\\x00\\x00\\x00\\x0b", escapeString(block));

        FilterBlockReader reader = new FilterBlockReader(policy, block);
        assertTrue(reader.keyMayMatch(0, "foo"));
        assertTrue(reader.keyMayMatch(100000, "foo"));
    }

    @Test
    public void testSingleChunk() {
        FilterBlockBuilder builder = new FilterBlockBuilder(policy);
        builder.startBlock(100);
        builder.addKey("foo");
        builder.addKey("bar");
        builder.addKey("box");
        builder.startBlock(200);
        builder.addKey("box");
        builder.startBlock(300);
        builder.addKey("hello");
        String block = builder.finish();
        FilterBlockReader reader = new FilterBlockReader(policy, block);
        assertTrue(reader.keyMayMatch(100, "foo"));
        assertTrue(reader.keyMayMatch(100, "bar"));
        assertTrue(reader.keyMayMatch(100, "box"));

        assertTrue(reader.keyMayMatch(100, "hello"));
        assertTrue(reader.keyMayMatch(100, "foo"));

        assertTrue(! reader.keyMayMatch(100, "missing"));
        assertTrue(! reader.keyMayMatch(100, "other"));
    }

    @Test
    public void testMultiChunk() {
        FilterBlockBuilder builder = new FilterBlockBuilder(policy);
        // First filter
        builder.startBlock(0);
        builder.addKey("foo");
        builder.startBlock(2000);
        builder.addKey("bar");

        // Second filter
        builder.startBlock(3100);
        builder.addKey("box");

        // Third filter is empty

        // Last filter
        builder.startBlock(9000);
        builder.addKey("box");
        builder.addKey("hello");

        String block = builder.finish();
        FilterBlockReader reader = new FilterBlockReader(policy, block);

        // Check first filter
        assertTrue(reader.keyMayMatch(0, "foo"));
        assertTrue(reader.keyMayMatch(2000, "bar"));
        assertTrue(! reader.keyMayMatch(0, "box"));
        assertTrue(! reader.keyMayMatch(0, "hello"));

        // Check second filter
        assertTrue(reader.keyMayMatch(3100, "box"));
        assertTrue(! reader.keyMayMatch(3100, "foo"));
        assertTrue(! reader.keyMayMatch(3100, "bar"));
        assertTrue(! reader.keyMayMatch(3100, "hello"));

        // Check third filter (empty)
        assertTrue(! reader.keyMayMatch(4100, "foo"));
        assertTrue(! reader.keyMayMatch(4100, "bar"));
        assertTrue(! reader.keyMayMatch(4100, "box"));
        assertTrue(! reader.keyMayMatch(4100, "hello"));

        // Check last filter
        assertTrue(reader.keyMayMatch(9000, "box"));
        assertTrue(reader.keyMayMatch(9000, "hello"));
        assertTrue(! reader.keyMayMatch(9000, "foo"));
        assertTrue(! reader.keyMayMatch(9000, "bar"));
    }

    private String escapeString(String value) {
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            if (c >= ' ' && c <= '~') {
                stringBuilder.append(c);
            } else {
                stringBuilder.append(String.format("\\x%02x", ((int)c) & 0xff));
            }
        }

        return stringBuilder.toString();
    }
}