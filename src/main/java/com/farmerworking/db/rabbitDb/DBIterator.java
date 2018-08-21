package com.farmerworking.db.rabbitDb;

/**
 * Created by John on 18/8/21.
 */
public interface DBIterator<K, V> {
  Status getStatus();

  boolean isValid();

  void next();

  void prev();

  void seekToFirst();

  void seekToLast();

  void seek(K key);

  K key();

  V value();
}
