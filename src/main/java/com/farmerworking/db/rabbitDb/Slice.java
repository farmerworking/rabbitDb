package com.farmerworking.db.rabbitDb;

import java.util.Arrays;

public class Slice implements Comparable<Slice> {

  public static Slice EMPTY_SLICE = new Slice();

  private char[] data;
  private int size;
  private int index;

  public Slice() {
    this.data = null;
    this.size = 0;
    this.index = 0;
  }

  public Slice(char[] data, int size, int index) {
    this.data = data;
    this.size = size;
    this.index = index;
  }

  public Slice(char[] data, int size) {
    this.data = data;
    this.size = size;
    this.index = 0;
  }

  public Slice(char[] data) {
    this.data = data;
    this.size = data.length;
    this.index = 0;
  }

  public Slice(String data) {
    this.data = data.toCharArray();
    this.size = data.length();
    this.index = 0;
  }

  public int getSize() {
    return size;
  }

  public char[] getData() {
    if (data == null) {
      return data;
    }

    if (index == 0 && data.length == size) {
      return data;
    } else {
      return Arrays.copyOfRange(data, index, index + size);
    }
  }

  public byte[] getBytes() {
    char[] result = getData();
    return result == null ? null : new String(result).getBytes();
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public char get(int n) {
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
    char[] result = getData();
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
    return ByteWiseComparator.getInstance().compare(this.getBytes(), slice.getBytes());
  }
}
