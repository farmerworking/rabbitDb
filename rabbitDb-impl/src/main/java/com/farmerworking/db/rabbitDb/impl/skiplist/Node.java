package com.farmerworking.db.rabbitDb.impl.skiplist;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class Node<T> {

    private final
    @Getter
    T item;
    private final
    @Getter
    Map<Integer, Node<T>> levelNext;
    private volatile
    @Setter
    @Getter
    Node<T> level0PrevNode;

    public Node(T item, int height, Node<T> level0PrevNode) {
        this.item = item;
        this.level0PrevNode = level0PrevNode;
        this.levelNext = new ConcurrentHashMap<>(height);
    }
}
