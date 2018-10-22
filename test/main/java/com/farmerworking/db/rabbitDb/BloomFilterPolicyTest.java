package com.farmerworking.db.rabbitDb;

import com.farmerworking.db.rabbitDb.sstable.FilterPolicy;
import com.farmerworking.db.rabbitDb.utils.Coding;
import org.junit.Before;
import org.junit.Test;

import java.util.Vector;

import static org.junit.Assert.*;

/**
 * Created by John on 18/10/15.
 */
public class BloomFilterPolicyTest {
    class BloomTest {
        private FilterPolicy policy;
        private Slice filter;
        private Vector<Slice> keys;

        public BloomTest() {
            this.policy = new BloomFilterPolicy(10);
            this.keys = new Vector<>();
            this.filter = Slice.EMPTY_SLICE;
        }

        public void reset() {
            keys.clear();
            this.filter = Slice.EMPTY_SLICE;
        }

        public void add(String key) {
            keys.add(new Slice(key));
        }

        public void add(Slice key) {
            keys.add(key);
        }

        public void build() {
            filter = new Slice(policy.createFilter(keys));
            keys.clear();
        }

        public int filterSize() {
            return filter.getSize();
        }

        public boolean matches(String key) {
            if (!keys.isEmpty()) {
                build();
            }

            return policy.keyMayMatch(new Slice(key), this.filter);
        }

        public boolean matches(Slice key) {
            if (!keys.isEmpty()) {
                build();
            }

            return policy.keyMayMatch(key, this.filter);
        }

        public double falsePositiveRate() {
            int result = 0;
            for (int i = 0; i < 10000; i++) {
                if (matches(key(i + 1000000000))) {
                    result++;
                }
            }

            return result / 10000.0;
        }
    }

    private BloomTest bloomTest;

    @Before
    public void setUp() throws Exception {
        this.bloomTest = new BloomTest();
    }

    @Test
    public void testEmptyFilter() throws Exception {
        assertTrue(!bloomTest.matches("hello"));
        assertTrue(!bloomTest.matches("world"));
    }

    @Test
    public void testSmall() throws Exception {
        bloomTest.add("hello");
        bloomTest.add("world");

        assertTrue(bloomTest.matches("hello"));
        assertTrue(bloomTest.matches("world"));
        assertTrue(!bloomTest.matches("x"));
        assertTrue(!bloomTest.matches("foo"));
    }

    @Test
    public void testVaryingLengths() throws Exception {
        int mediocreFilters = 0;
        int goodFilters = 0;


        for (int length = 1; length <= 10000 ; length = nextLength(length)){
            bloomTest.reset();
            for (int i = 0; i < length; i++) {
                bloomTest.add(key(i));
            }

            bloomTest.build();

            assertTrue(bloomTest.filterSize() <= (length * 10 / 8) + 40);

            for (int i = 0; i < length; i++) {
                assertTrue(bloomTest.matches(key(i)));
            }

            double rate = bloomTest.falsePositiveRate();
            assertTrue(rate <= 0.02);
            if (rate > 0.0125) {
                mediocreFilters ++;
            } else {
                goodFilters ++;
            }
        }

        assertTrue(mediocreFilters <= goodFilters / 5);
    }

    private int nextLength(int length) {
        if (length < 10) {
            length += 1;
        } else if (length < 100) {
            length += 10;
        } else if (length < 1000) {
            length += 100;
        } else {
            length += 1000;
        }
        return length;
    }

    private Slice key(int i) {
        char[] buffer = new char[4];
        Coding.encodeFixed32(buffer, 0, i);
        return new Slice(buffer);
    }
}