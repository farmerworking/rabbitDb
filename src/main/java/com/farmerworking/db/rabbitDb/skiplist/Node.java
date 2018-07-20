package com.farmerworking.db.rabbitDb.skiplist;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;

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
