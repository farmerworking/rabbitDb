package com.farmerworking.db.rabbitDb.skiplist;

public class SkipListIterator<T> {

  private final SkipList<T> skipList;
  private Node<T> current;

  SkipListIterator(SkipList<T> skipList) {
    this.skipList = skipList;
    this.current = null;
  }

  public boolean valid() {
    return current != skipList.head && current != skipList.tail && current != null;
  }

  public T key() {
    assert valid();
    return current.getItem();
  }

  public void next() {
    assert current != null && current != skipList.tail;
    this.current = current.getLevelNext().get(0);
  }

  public void prev() {
    assert current != null && current != skipList.head;
    this.current = current.getLevel0PrevNode();
  }

  public void seekToFirst() {
    this.current = skipList.head.getLevelNext().get(0);
  }

  public void seekToLast() {
    this.current = skipList.tail.getLevel0PrevNode();
  }

  public void seek(T key) {
    this.current = skipList.findGreaterOrEqual(key, null);
  }
}
