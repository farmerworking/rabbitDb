package com.farmerworking.db.rabbitDb;

import com.google.common.primitives.SignedBytes;
import java.util.Arrays;

public class Slice implements Comparable<Slice> {

  public static Slice EMPTY_SLICE = new Slice();

  private byte[] data;
  private int size;
  private int index;

  public Slice() {
    this.data = null;
    this.size = 0;
    this.index = 0;
  }

  public Slice(byte[] data, int size) {
    this.data = data;
    this.size = size;
    this.index = 0;
  }

  public Slice(byte[] data) {
    this.data = data;
    this.size = data.length;
    this.index = 0;
  }

  public Slice(String data) {
    this.data = data.getBytes();
    this.size = data.length();
    this.index = 0;
  }

  public int getSize() {
    return size;
  }

  public byte[] getData() {
    if (data == null) {
      return data;
    }

    if (index == 0 && data.length == size) {
      return data;
    } else {
      return Arrays.copyOfRange(data, index, index + size);
    }
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public byte get(int n) {
    assert n < size;
    return data[index + n];
  }

  public void clear() {
    this.data = null;
    this.size = 0;
    this.index = 0;
  }

  public void removePrefix(int n) {
    assert n <= size;
    index += n;
    size -= n;
  }

  public boolean startsWith(Slice slice) {
    if (this == slice) {
      return true;
    }
    if (slice == null) {
      return false;
    }

    if (size < slice.size) {
      return false;
    }

    for (int i = 0; i < slice.size; i++) {
      if (data[index + i] != slice.data[slice.index + i]) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    byte[] result = getData();
    return result == null ? "null" : new String(getData());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Slice slice = (Slice) o;

    return size == slice.size && Arrays.equals(this.getData(), slice.getData());
  }

  @Override
  public int compareTo(Slice slice) {
    int minLength = Math.min(this.size, slice.size);
    for (int i = 0; i < minLength; i++) {
      int result = SignedBytes.compare(this.data[this.index + i], slice.data[slice.index + i]);
      if (result != 0) {
        return result;
      }
    }
    return this.size - slice.size;
  }
}
