package com.farmerworking.db.rabbitDb.harness;

import com.farmerworking.db.rabbitDb.utils.TestUtils;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * Created by John on 18/10/18.
 */
public class TableHarnessTest {
    List<TestArgs> TEST_ARGS_LIST = Lists.newArrayList(
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
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.test(verbose);
        }
    }

    @Test
    public void testSimpleEmptyKey() throws Exception {
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.add("", "v");
            harness.test(verbose);
        }
    }

    @Test
    public void testSimpleSingle() throws Exception {
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.add("abc", "v");
            harness.test(verbose);
        }
    }

    @Test
    public void testSimpleMulti() throws Exception {
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.add("abc", "v");
            harness.add("abcd", "v");
            harness.add("ac", "v2");
            harness.test(verbose);
        }
    }

    @Test
    public void testSimpleSpecialKey() throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append((char) 255);
        stringBuilder.append((char) 255);
        String s = stringBuilder.toString();
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            harness.add(s, "v3");
            harness.test(verbose);
        }
    }

    @Test
    public void testRandomized() throws Exception {
        for (int i = 0; i < TEST_ARGS_LIST.size(); i++) {
            harness.init(TEST_ARGS_LIST.get(i));
            for (int num_entries = 0; num_entries < 2000; num_entries += (num_entries < 50 ? 1 : 200)) {
                if ((num_entries % 10) == 0 && verbose) {
                    System.out.println(String.format("case %d of %d: num_entries = %d", (i + 1), TEST_ARGS_LIST.size(), num_entries));
                }
                Random random = new Random();
                for (int e = 0; e < num_entries; e++) {
                    harness.add(TestUtils.randomKey(random.nextInt(4)), TestUtils.randomString(random.nextInt(5)));
                }
                harness.test(verbose);
            }
        }
    }
}