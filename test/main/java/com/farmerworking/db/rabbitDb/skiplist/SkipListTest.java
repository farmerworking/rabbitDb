package com.farmerworking.db.rabbitDb.skiplist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Created by John on 18/7/20.
 */
public class SkipListTest {

  private synchronized void runConcurrent(int run) throws InterruptedException {
    final Random random = new Random(run * 100);
    int N = 1000;
    int kSize = 1000;
    for (int i = 0; i < N; i++) {
      if ((i % 100) == 0) {
        System.out.println(String.format("Run %d of %d", i, N));
      }
      final TestState testState = new TestState();

      (new Thread(() -> {
        testState.changeState("RUNNING");
        while (!testState.quit) {
          testState.concurrentTest.readStep(random);
        }
        testState.changeState("DONE");
      })).start();
      testState.waitState("RUNNING");
      for (int j = 0; j < kSize; j++) {
        testState.concurrentTest.writeStep(random);
      }
      testState.quit = true;
      testState.waitState("DONE");
    }
  }

  @Test
  public void testConcurrentWithoutThreads() {
    ConcurrentTest test = new ConcurrentTest();
    Random random = new Random();
    for (int i = 0; i < 10000; i++) {
      test.readStep(random);
      test.writeStep(random);
    }
  }

  @Test
  public void testConcurrent1() throws InterruptedException {
    runConcurrent(1);
  }

  @Test
  public void testConcurrent2() throws InterruptedException {
    runConcurrent(2);
  }

  @Test
  public void testConcurrent3() throws InterruptedException {
    runConcurrent(3);
  }

  @Test
  public void testConcurrent4() throws InterruptedException {
    runConcurrent(4);
  }

  @Test
  public void testConcurrent5() throws InterruptedException {
    runConcurrent(5);
  }

  @Test
  public void testEmpty() {
    SkipList<Long> list = new SkipList<>(new TmpComparator());
    assertTrue(!list.contains(10L));

    SkipListIterator<Long> skipListIterator = list.iterator();
    assertTrue(!skipListIterator.valid());
    skipListIterator.seekToFirst();
    assertTrue(!skipListIterator.valid());
    skipListIterator.seek(100L);
    assertTrue(!skipListIterator.valid());
    skipListIterator.seekToLast();
    assertTrue(!skipListIterator.valid());
  }

  @Test
  public void testHeadNextAndTailPrev() {
    SkipList<Long> list = new SkipList<>(new TmpComparator());
    list.insert(1000L);

    SkipListIterator<Long> iter = list.iterator();
    iter.seekToFirst();
    assertTrue(iter.valid());
    assertEquals(1000L, iter.key().longValue());

    iter.prev();
    assertFalse(iter.valid());
    iter.next();
    assertTrue(iter.valid());
    assertEquals(1000L, iter.key().longValue());

    iter.next();
    assertFalse(iter.valid());
    iter.prev();
    assertTrue(iter.valid());
    assertEquals(1000L, iter.key().longValue());
  }

  @Test(expected = AssertionError.class)
  public void testHeadPrevException() {
    SkipList<Long> list = new SkipList<>(new TmpComparator());
    list.insert(1000L);

    SkipListIterator<Long> iter = list.iterator();
    iter.seekToFirst();
    assertTrue(iter.valid());
    assertEquals(1000L, iter.key().longValue());

    iter.prev();
    assertFalse(iter.valid());
    iter.prev();
  }

  @Test(expected = AssertionError.class)
  public void testTailNextException() {
    SkipList<Long> list = new SkipList<>(new TmpComparator());
    list.insert(1000L);

    SkipListIterator<Long> iter = list.iterator();
    iter.seekToFirst();
    assertTrue(iter.valid());
    assertEquals(1000L, iter.key().longValue());

    iter.next();
    assertFalse(iter.valid());
    iter.next();
  }

  @Test
  public void testInsertAndLookup() {
    int N = 2000;
    int R = 5000;
    SortedSet<Long> keys = new TreeSet<>();

    Random random = new Random();
    SkipList<Long> list = new SkipList<>(new TmpComparator());
    for (int i = 0; i < N; i++) {
      Long key = (long) (Math.abs(random.nextInt()) % R);

      if (keys.add(key)) {
        list.insert(key);
      }
    }

    for (int i = 0; i < R; i++) {
      if (list.contains((long) i)) {
        assertTrue(keys.contains((long) i));
      } else {
        assertFalse(keys.contains((long) i));
      }
    }

    // Simple skipListIterator tests
    SkipListIterator<Long> skipListIterator = list.iterator();
    assertTrue(!skipListIterator.valid());

    skipListIterator.seek(0L);
    assertTrue(skipListIterator.valid());
    assertEquals(keys.first(), skipListIterator.key());

    skipListIterator.seekToFirst();
    assertTrue(skipListIterator.valid());
    assertEquals(keys.first(), skipListIterator.key());

    skipListIterator.seekToLast();
    assertTrue(skipListIterator.valid());
    assertEquals(keys.last(), skipListIterator.key());

    // Forward iteration test
    for (int i = 0; i < R; i++) {
      SkipListIterator<Long> iter = list.iterator();
      iter.seek((long) i);

      // Compare against model skipListIterator
      java.util.Iterator<Long> iterator1 = (keys.tailSet((long) i)).iterator();
      for (int j = 0; j < 3; j++) {
        if (iterator1.hasNext()) {
          assertTrue(iter.valid());
          assertEquals(iterator1.next(), iter.key());
          iter.next();
        } else {
          assertTrue(!iter.valid());
          break;
        }
      }
    }

    // Backward iteration test
    SkipListIterator<Long> iter = list.iterator();
    iter.seekToLast();

    // Compare against model skipListIterator
    SortedSet<Long> reverseKeys = new TreeSet<>(Collections.reverseOrder());
    reverseKeys.addAll(keys);
    java.util.Iterator<Long> iterator1 = reverseKeys.iterator();
    while (iterator1.hasNext()) {
      assertTrue(iter.valid());
      assertEquals(iterator1.next(), iter.key());
      iter.prev();
    }
    assertTrue(!iter.valid());
  }

  // We want to make sure that with a single writer and multiple
  // concurrent readers (with no synchronization other than when a
  // reader's iterator is created), the reader always observes all the
  // data that was present in the skip list when the iterator was
  // constructor.  Because insertions are happening concurrently, we may
  // also observe new values that were inserted since the iterator was
  // constructed, but we should never miss any values that were present
  // at iterator construction time.
  //
  // We generate multi-part keys:
  //     <key,gen,hash>
  // where:
  //     key is in range [0..K-1]
  //     gen is a generation number for key
  //     hash is hash(key,gen)
  //
  // The insertion code picks a random key, sets gen to be 1 + the last
  // generation number inserted for that key, and sets hash to Hash(key,gen).
  //
  // At the beginning of a read, we snapshot the last inserted
  // generation number for each key.  We then iterate, including random
  // calls to Next() and Seek().  For every key we encounter, we
  // check that it is either expected given the initial snapshot or has
  // been concurrently added since the iterator started.
  class ConcurrentTest {

    private static final int K = 4;
    private ConcurrentHashMap<Integer, Integer> current;
    private SkipList<ConcurrentTestNode> skipList;

    ConcurrentTest() {
      this.skipList = new SkipList<>(new ConcurrentTestComparator());
      this.current = new ConcurrentHashMap<>();
      for (int i = 0; i < K; i++) {
        current.put(i, 0);
      }
    }

    // REQUIRES: External synchronization
    void writeStep(Random random) {
      Integer k = Math.abs(random.nextInt()) % K;
      Integer g = current.get(k) + 1;
      skipList.insert(new ConcurrentTestNode(k, g));
      current.put(k, g);
    }

    void readStep(Random random) {
      // Remember the initial committed state of the skiplist.
      ConcurrentHashMap<Integer, Integer> initialState = snapshot();

      SkipListIterator<ConcurrentTestNode> iter = skipList.iterator();
      ConcurrentTestComparator comparator = new ConcurrentTestComparator();

      ConcurrentTestNode testNode = null;
      while (true) {
        Pair<ConcurrentTestNode, ConcurrentTestNode> pair = randomSeekOrNext(random, iter,
            comparator, testNode);
        testNode = pair.getLeft();
        ConcurrentTestNode currentNode = pair.getRight();

        assertTrue(currentNode.toString(), isValidKey(currentNode)); // check read partial record
        assertTrue("should not go backwards", comparator.compare(testNode, currentNode) <= 0);

        // Verify that everything in [pos,current) was not present in
        // initial_state.
        while (comparator.compare(testNode, currentNode) < 0) {
          assertTrue(testNode.key < K);

          // for seek case, testNode.generation == 0
          // for next case:
          // 1. testNode.key == preTestNode.key && testNode.generation == preTestNode.generation + 1
          // 2. currentNode == preCurrentNode.next()
          // 3. preTestNode ==  preCurrentNode
          //
          // so, here, testNode.key = currentNode.key - 1, testNode.generation = preCurrentNode.generation + 1
          // which should larger than snapshot
          // The node with generation(preCurrentNode.generation + 1) is missing if assert failed
          assertTrue(
              testNode.generation == 0 // true for seek case
                  || testNode.generation > initialState.get(testNode.key));

          // Advance to next key in the valid key space
          if (testNode.key < currentNode.key) {
            testNode = makeKey(testNode.key + 1, 0);
          } else {
            testNode = makeKey(testNode.key, testNode.generation + 1);
          }
        }

        if (!iter.valid()) {
          break;
        }
      }
    }

    // for the first time, it will be a seek. After that random choose seek or nexRRRR
    // for seek, return a random node, iter.key point to the least node larger or equal than return node
    // for next, return a node with previous node's key and generation + 1, iter.key point to the next node;
    private Pair<ConcurrentTestNode, ConcurrentTestNode> randomSeekOrNext(Random random,
        SkipListIterator<ConcurrentTestNode> iter,
        ConcurrentTestComparator comparator,
        ConcurrentTestNode pos) {
      if (pos == null // first time
          || Math.abs(random.nextInt()) % 2 > 0) {
        ConcurrentTestNode newTarget = randomTarget(random);
        if (comparator.compare(newTarget, pos) > 0) {
          pos = newTarget;
          iter.seek(newTarget);
        }
      } else {
        iter.next();
        pos = makeKey(pos.key, pos.generation + 1);
      }
      if (iter.valid()) {
        return Pair.of(pos, iter.key());
      } else {
        return Pair.of(pos, makeKey(K, 0));
      }
    }

    private ConcurrentHashMap<Integer, Integer> snapshot() {
      ConcurrentHashMap<Integer, Integer> initialState = new ConcurrentHashMap<>();
      for (int i = 0; i < K; i++) {
        initialState.put(i, current.get(i));
      }
      return initialState;
    }

    private boolean isValidKey(ConcurrentTestNode k) {
      return k.hash == Arrays.hashCode(new Integer[]{k.key, k.generation});
    }

    private ConcurrentTestNode randomTarget(Random random) {
      switch (Math.abs(random.nextInt()) % 10) {
        case 0:
          // Seek to beginning
          return makeKey(0, 0);
        case 1:
          // Seek to end
          return makeKey(K, 0);
        default:
          // Seek to middle
          return makeKey(Math.abs(random.nextInt()) % K, 0);
      }
    }

    private ConcurrentTestNode makeKey(Integer k, Integer g) {
      return new ConcurrentTestNode(k, g);
    }

    class ConcurrentTestNode {

      Integer key;
      Integer generation;
      Integer hash;

      ConcurrentTestNode(Integer key, Integer generation) {
        this.key = key;
        this.generation = generation;
        this.hash = Arrays.hashCode(new Integer[]{key, generation});
      }
    }

    class ConcurrentTestComparator implements Comparator<ConcurrentTestNode> {

      @Override
      public int compare(ConcurrentTestNode o1, ConcurrentTestNode o2) {
        if (o2 == null) {
          return 1;
        }

        if (o1.key > o2.key) {
          return 1;
        } else if (o1.key < o2.key) {
          return -1;
        } else {
          if (o1.generation > o2.generation) {
            return 1;
          } else if (o1.generation < o2.generation) {
            return -1;
          } else {
            return 0;
          }
        }
      }
    }
  }

  class TmpComparator implements Comparator<Long> {

    @Override
    public int compare(Long a, Long b) {
      if (a < b) {
        return -1;
      } else if (a > b) {
        return +1;
      } else {
        return 0;
      }
    }
  }

  class TestState {

    ConcurrentTest concurrentTest;
    volatile boolean quit;
    private volatile String state;

    TestState() {
      this.state = "STARTING";
      this.concurrentTest = new ConcurrentTest();
      this.quit = false;
    }

    synchronized void waitState(String state) throws InterruptedException {
      while (!this.state.equalsIgnoreCase(state)) {
        wait();
      }
    }

    synchronized void changeState(String state) {
      this.state = state;
      notify();
    }
  }
}