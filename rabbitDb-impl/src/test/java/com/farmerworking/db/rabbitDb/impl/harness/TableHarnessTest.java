package com.farmerworking.db.rabbitDb.impl.harness;

import com.farmerworking.db.rabbitDb.impl.BloomFilterPolicy;
import com.farmerworking.db.rabbitDb.impl.utils.TestUtils;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * Created by John on 18/10/18.
 */
public class TableHarnessTest {
    List<TestArgs> TEST_ARGS_LIST = Lists.newArrayList(
            new TestArgs("TABLE_TEST", false, 16),
            new TestArgs("TABLE_TEST", false, 1),
            new TestArgs("TABLE_TEST", false, 1024),
            new TestArgs("TABLE_TEST", true, 16),
            new TestArgs("TABLE_TEST", true, 1),
            new TestArgs("TABLE_TEST", true, 1024),

            new TestArgs("TABLE_TEST", false, 16, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", false, 1, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", false, 1024, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", true, 16, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", true, 1, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", true, 1024, new BloomFilterPolicy(10)),

            new TestArgs("TABLE_TEST", false, 16, true),
            new TestArgs("TABLE_TEST", false, 1, true),
            new TestArgs("TABLE_TEST", false, 1024, true),
            new TestArgs("TABLE_TEST", true, 16, true),
            new TestArgs("TABLE_TEST", true, 1, true),
            new TestArgs("TABLE_TEST", true, 1024, true),

            new TestArgs("TABLE_TEST", false, 16, true, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", false, 1, true, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", false, 1024, true, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", true, 16, true, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", true, 1, true, new BloomFilterPolicy(10)),
            new TestArgs("TABLE_TEST", true, 1024, true, new BloomFilterPolicy(10)),

            new TestArgs("DB_TEST", false, 16),
            new TestArgs("DB_TEST", true, 16),

            new TestArgs("MEMTABLE_TEST", false, 16),
            new TestArgs("MEMTABLE_TEST", true, 16),

            new TestArgs("BLOCK_TEST", false, 16),
            new TestArgs("BLOCK_TEST", false, 1),
            new TestArgs("BLOCK_TEST", false, 1024),
            new TestArgs("BLOCK_TEST", true, 16),
            new TestArgs("BLOCK_TEST", true, 1),
            new TestArgs("BLOCK_TEST", true, 1024)
    );

    private Harness harness;
    private boolean verbose;

    @Before
    public void setUp() throws Exception {
        harness = new Harness();
        verbose = false;
    }

    @Test
    public void testEmtpy() throws Exception {
        String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.test(verbose);
            System.out.println(TEST_ARGS_LIST.get(i).getName() + " " + methodName);
        }
    }

    @Test
    public void testSimpleEmptyKey() throws Exception {
        String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.add("", "v");
            harness.test(verbose);
            System.out.println(TEST_ARGS_LIST.get(i).getName() + " " + methodName);
        }
    }

    @Test
    public void testSimpleSingle() throws Exception {
        String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.add("abc", "v");
            harness.test(verbose);
            System.out.println(TEST_ARGS_LIST.get(i).getName() + " " + methodName);
        }
    }

    @Test
    public void testSimpleMulti() throws Exception {
        String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.add("abc", "v");
            harness.add("abcd", "v");
            harness.add("ac", "v2");
            harness.test(verbose);
            System.out.println(TEST_ARGS_LIST.get(i).getName() + " " + methodName);
        }
    }

    @Test
    public void testSimpleSpecialKey() throws Exception {
        String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append((char) 255);
        stringBuilder.append((char) 255);
        String s = stringBuilder.toString();
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.add(s, "v3");
            harness.test(verbose);
            System.out.println(TEST_ARGS_LIST.get(i).getName() + " " + methodName);
        }
    }

    @Test
    public void testRandomized() throws Exception {
        String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            for (int num_entries = 0; num_entries < 2000; num_entries += (num_entries < 50 ? 1 : 200)) {
                if ((num_entries % 10) == 0) {
                    System.out.println(String.format("case %d of %d: num_entries = %d", (i + 1), TEST_ARGS_LIST.size(), num_entries));
                }
                Random random = new Random();
                for (int e = 0; e < num_entries; e++) {
                    harness.add(TestUtils.randomKey(random.nextInt(4)), TestUtils.randomString(random.nextInt(5)));
                }
                harness.test(verbose);
            }
            System.out.println(TEST_ARGS_LIST.get(i).getName() + " " + methodName);
        }
    }
}