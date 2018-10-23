package com.farmerworking.db.rabbitDb.impl.skiplist;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SkipList<T> {

    private static final int MAX_HEIGHT = 12;
    // immutable after construction
    final Node<T> head;
    final Node<T> tail;
    private final Comparator<T> comparator;
    private final Random random;
    private volatile int maxHeight;
    private volatile long memoryUsage;

    public SkipList(Comparator<T> comparator) {
        this.comparator = comparator;
        this.maxHeight = 1;
        this.memoryUsage = 0;
        this.random = new Random();

        head = new Node<>(null, MAX_HEIGHT, null);
        tail = new Node<>(null, MAX_HEIGHT, head);

        for (int i = 0; i < MAX_HEIGHT; i++) {
            head.getLevelNext().put(i, tail);
        }
    }

    public synchronized void insert(T item) {
        Map<Integer, Node<T>> prev = new HashMap<>(MAX_HEIGHT);
        Node<T> greaterOrEqualNode = findGreaterOrEqual(item, prev);

        // Our data structure does not allow duplicate insertion
        assert
                greaterOrEqualNode == tail || comparator.compare(greaterOrEqualNode.getItem(), item) != 0;

        int height = randomHeight();
        if (height > maxHeight) {
            for (int i = maxHeight; i < height; i++) {
                prev.put(i, head);
            }

            maxHeight = height;
        }

        Node<T> newNode = new Node<>(item, height, prev.get(0));
        prev.get(0).getLevelNext().get(0).setLevel0PrevNode(newNode);

        for (int level = 0; level < height; level++) {
            newNode.getLevelNext().put(level, prev.get(level).getLevelNext().get(level));
            prev.get(level).getLevelNext().put(level, newNode);
        }

        if (item instanceof Sizeable) {
            memoryUsage += ((Sizeable) item).approximateMemoryUsage();
        } else {
            memoryUsage += 1; // for unit test only
        }
    }

    public boolean contains(T item) {
        Node<T> node = findGreaterOrEqual(item, null);
        return node != tail && comparator.compare(node.getItem(), item) == 0;
    }

    public SkipListIterator<T> iterator() {
        return new SkipListIterator<>(this);
    }

    @Override
    public String toString() {
        // for debug purpose
        SkipListIterator<T> iter = iterator();

        StringBuilder builder = new StringBuilder();
        iter.seekToFirst();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            T entry = iter.key();
            builder.append("-->");
            builder.append(entry.toString());
            builder.append("\n");
        }
        return builder.toString();
    }

    synchronized Node<T> findGreaterOrEqual(T item, Map<Integer, Node<T>> prev) {
        Node<T> prevNode = head;
        int level = maxHeight - 1;
        while (true) {
            Node<T> nextNode = prevNode.getLevelNext().get(level);
            if (isKeyAfterNode(item, nextNode)) {
                prevNode = nextNode;
            } else {
                if (prev != null) {
                    prev.put(level, prevNode);
                }

                if (level == 0) {
                    return nextNode;
                } else {
                    level--;
                }
            }
        }
    }

    private int randomHeight() {
        int height = 1;
        while (height < MAX_HEIGHT && (random.nextInt() % 4 == 0)) {
            height++;
        }
        assert height > 0;
        assert height <= MAX_HEIGHT;
        return height;
    }

    private boolean isKeyAfterNode(T item, Node<T> node) {
        return node != tail && comparator.compare(node.getItem(), item) < 0;
    }

    public long approximateMemoryUsage() {
        return memoryUsage;
    }
}
