package com.farmerworking.db.rabbitDb.impl.harness;

import com.farmerworking.db.rabbitDb.api.CompressionType;
import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.api.Slice;
import com.farmerworking.db.rabbitDb.impl.harness.block.BlockConstructor;
import com.farmerworking.db.rabbitDb.impl.harness.memtable.MemTableConstructor;
import com.farmerworking.db.rabbitDb.impl.harness.db.DBConstructor;
import com.farmerworking.db.rabbitDb.impl.sstable.TableConstructor;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class Harness {
    private Options options;
    private Constructor constructor;

    public void init(TestArgs args) {
        options = new Options();
        options.blockRestartInterval(args.getRestartInterval());
        options.blockSize(256);
        options.paranoidChecks(true);

        if (args.isReverseCompare()) {
            options.comparator(new ReverseKeyComparator());
        } else {
            options.comparator(ByteWiseComparator.getInstance());
        }

        if (args.isCompress()) {
            options.compressionType(CompressionType.SNAPPY);
        }

        if (args.getName().equals("BLOCK_TEST")) {
            this.constructor = new BlockConstructor(options.comparator());
        } else if (args.getName().equals("MEMTABLE_TEST")) {
            this.constructor = new MemTableConstructor(options.comparator());
        } else if (args.getName().equals("DB_TEST")) {
            this.constructor = new DBConstructor(options.comparator());
        } else if (args.getName().equals("TABLE_TEST")) {
            this.constructor = new TableConstructor(options.comparator());
        } else {
            throw new RuntimeException("no support constructor for " + args.getName());
        }
    }

    public void add(String key, String value) {
        constructor.add(key, value);
    }

    public void test(boolean verbose) {
        Pair<Vector<String>, ConcurrentSkipListMap<String, String>> pair = constructor.finish(options);
        Vector<String> keys = pair.getLeft();
        ConcurrentSkipListMap<String, String> data = pair.getRight();

        if (verbose) {
            System.out.println(String.format("keys: %s, data: %s", (new Gson()).toJson(keys), (new Gson()).toJson(data)));
        }

        testForwardScan(keys, data, verbose);
        testBackwardScan(keys, data, verbose);
        testRandomAccess(keys, data, verbose);
    }

    private void testRandomAccess(Vector<String> keys, ConcurrentSkipListMap<String, String> data, boolean verbose) {
        DBIterator iter = constructor.newIterator();
        assertTrue(!iter.isValid());

        Integer index = null;
        Random random = new Random();
        for (int i = 0; i < 200; i++) {
            int toss = random.nextInt(5);

            switch (toss) {
                case 0:
                    if (iter.isValid()) {
                        if (verbose) System.out.println("Next " + index);
                        iter.next();

                        if (iter.isValid()) {
                            index ++;
                            String key = keys.get(index);
                            assertEquals(toString(key, data.get(key)), toString(iter.key().toString(), iter.value().toString()));
                        }
                    }
                    break;
                case 1:
                    if (verbose) System.out.println("SeekToFirst " + index);
                    iter.seekToFirst();
                    if (iter.isValid()) {
                        String key = keys.firstElement();
                        index = 0;
                        assertEquals(toString(key, data.get(key)), toString(iter.key().toString(), iter.value().toString()));
                    }
                    break;
                case 2:
                    if (iter.isValid()) {
                        String key = pickRandomKey(random, keys);
                        if (verbose) System.out.println("Seek " + key + ", " + index);

                        iter.seek(new Slice(key));
                        if (iter.isValid()) {
                            Map.Entry<String, String> entry = data.ceilingEntry(key);
                            index = keys.indexOf(entry.getKey());
                            assertEquals(toString(entry.getKey(), entry.getValue()), toString(iter.key().toString(), iter.value().toString()));
                        }
                    }
                    break;
                case 3:
                    if (iter.isValid()) {
                        if (verbose) System.out.println("Prev " + index);
                        iter.prev();

                        if (iter.isValid()) {
                            index --;
                            String key = keys.get(index);
                            assertEquals(toString(key, data.get(key)), toString(iter.key().toString(), iter.value().toString()));
                        }
                    }
                    break;
                case 4:
                    if (verbose) System.out.println("SeekToLast " + index);
                    iter.seekToLast();
                    if (iter.isValid()) {
                        String key = keys.lastElement();
                        index = keys.size() - 1;
                        assertEquals(toString(key, data.get(key)), toString(iter.key().toString(), iter.value().toString()));
                    }
                    break;
            }
        }
    }

    private void testForwardScan(Vector<String> keys, ConcurrentSkipListMap<String, String> data, boolean verbose) {
        DBIterator iter = constructor.newIterator();
        assertTrue(!iter.isValid());
        iter.seekToFirst();

        for(String key : keys) {
            assertTrue(iter.isValid());
            assertEquals(toString(key, data.get(key)), toString(iter.key().toString(), iter.value().toString()));
            iter.next();
        }

        assertTrue(!iter.isValid());
    }

    private void testBackwardScan(Vector<String> keys, ConcurrentSkipListMap<String, String> data, boolean verbose) {
        DBIterator iter = constructor.newIterator();
        assertTrue(!iter.isValid());
        iter.seekToLast();

        List<String> reverseKeys = Lists.reverse(keys);
        for(String key : reverseKeys) {
            assertTrue(iter.isValid());
            assertEquals(toString(key, data.get(key)), toString(iter.key().toString(), iter.value().toString()));
            iter.prev();
        }

        assertTrue(!iter.isValid());
    }

    private String toString(String key, String value) {
        return "'" + key + "->" + value + "'";
    }

    private String pickRandomKey(Random random, Vector<String> keys) {
        if (keys.isEmpty()) {
            return "foo";
        } else {
            int index = random.nextInt(keys.size());
            String result = keys.get(index);

            switch (random.nextInt(3)) {
                case 0:
                    // return existing key
                    break;
                case 1:
                    // Attempt to return something smaller than an existing key
                    if (!result.isEmpty() && result.charAt(result.length() - 1) > '\0') {
                        char[] chars = result.toCharArray();
                        chars[chars.length - 1] = (char)((int)chars[chars.length - 1] - 1);
                        result = new String(chars);
                    }
                    break;
                case 2:
                    // Return something larger than an existing key
                    if (options.comparator() instanceof ByteWiseComparator) {
                        result = result + '\0';
                    } else {
                        assert(options.comparator() instanceof ReverseKeyComparator);
                        result = '\0' + result;
                    }
                    break;
            }

            return result;
        }
    }
}
