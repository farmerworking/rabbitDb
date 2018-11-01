package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.FilterPolicy;
import com.farmerworking.db.rabbitDb.api.Slice;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import com.farmerworking.db.rabbitDb.impl.utils.Hash;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class FilterBlockTest {

    class TestHashFilter implements FilterPolicy {
        @Override
        public String createFilter(List<Slice> keys) {
            StringBuilder stringBuilder = new StringBuilder();

            for(Slice s : keys) {
                Coding.putFixed32(stringBuilder, Hash.hash(s.getData(), 1));
            }

            return stringBuilder.toString();
        }

        @Override
        public boolean keyMayMatch(Slice key, Slice filter) {
            int hash = Hash.hash(key.getData(), 1);
            char[] data = filter.getData();
            for (int i = 0; i + 4 <= filter.getSize(); i+=4) {
                if (hash == Coding.decodeFixed32(data, i).getRight()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String name() {
            return "TestHashFilter";
        }
    }

    private FilterPolicy policy;

    @Before
    public void setUp() throws Exception {
        this.policy = new TestHashFilter();
    }

    @Test
    public void testEmptyBuilder() {
        FilterBlockBuilder builder = new FilterBlockBuilder(policy);
        Slice block = builder.finish();
        assertEquals("\\x00\\x00\\x00\\x00\\x0b", escapeString(block));

        FilterBlockReader reader = new FilterBlockReader(policy, block);
        assertTrue(reader.keyMayMatch(0, new Slice("foo")));
        assertTrue(reader.keyMayMatch(100000, new Slice("foo")));
    }

    @Test
    public void testSingleChunk() {
        FilterBlockBuilder builder = new FilterBlockBuilder(policy);
        builder.startBlock(100);
        builder.addKey(new Slice("foo"));
        builder.addKey(new Slice("bar"));
        builder.addKey(new Slice("box"));
        builder.startBlock(200);
        builder.addKey(new Slice("box"));
        builder.startBlock(300);
        builder.addKey(new Slice("hello"));
        Slice block = builder.finish();
        FilterBlockReader reader = new FilterBlockReader(policy, block);
        assertTrue(reader.keyMayMatch(100, new Slice("foo")));
        assertTrue(reader.keyMayMatch(100, new Slice("bar")));
        assertTrue(reader.keyMayMatch(100, new Slice("box")));

        assertTrue(reader.keyMayMatch(100, new Slice("hello")));
        assertTrue(reader.keyMayMatch(100, new Slice("foo")));

        assertTrue(! reader.keyMayMatch(100, new Slice("missing")));
        assertTrue(! reader.keyMayMatch(100, new Slice("other")));
    }

    @Test
    public void testMultiChunk() {
        FilterBlockBuilder builder = new FilterBlockBuilder(policy);
        // First filter
        builder.startBlock(0);
        builder.addKey(new Slice("foo"));
        builder.startBlock(2000);
        builder.addKey(new Slice("bar"));

        // Second filter
        builder.startBlock(3100);
        builder.addKey(new Slice("box"));

        // Third filter is empty

        // Last filter
        builder.startBlock(9000);
        builder.addKey(new Slice("box"));
        builder.addKey(new Slice("hello"));
        
        Slice block = builder.finish();
        FilterBlockReader reader = new FilterBlockReader(policy, block);

        // Check first filter
        assertTrue(reader.keyMayMatch(0, new Slice("foo")));
        assertTrue(reader.keyMayMatch(2000, new Slice("bar")));
        assertTrue(! reader.keyMayMatch(0, new Slice("box")));
        assertTrue(! reader.keyMayMatch(0, new Slice("hello")));

        // Check second filter
        assertTrue(reader.keyMayMatch(3100, new Slice("box")));
        assertTrue(! reader.keyMayMatch(3100, new Slice("foo")));
        assertTrue(! reader.keyMayMatch(3100, new Slice("bar")));
        assertTrue(! reader.keyMayMatch(3100, new Slice("hello")));

        // Check third filter (empty)
        assertTrue(! reader.keyMayMatch(4100, new Slice("foo")));
        assertTrue(! reader.keyMayMatch(4100, new Slice("bar")));
        assertTrue(! reader.keyMayMatch(4100, new Slice("box")));
        assertTrue(! reader.keyMayMatch(4100, new Slice("hello")));

        // Check last filter
        assertTrue(reader.keyMayMatch(9000, new Slice("box")));
        assertTrue(reader.keyMayMatch(9000, new Slice("hello")));
        assertTrue(! reader.keyMayMatch(9000, new Slice("foo")));
        assertTrue(! reader.keyMayMatch(9000, new Slice("bar")));
    }

    private String escapeString(Slice value) {
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < value.getSize(); i++) {
            char c = value.get(i);

            if (c >= ' ' && c <= '~') {
                stringBuilder.append(c);
            } else {
                stringBuilder.append(String.format("\\x%02x", ((int)c) & 0xff));
            }
        }

        return stringBuilder.toString();
    }
}