package com.farmerworking.db.rabbitDb;

import java.util.Arrays;

public class Slice implements Comparable<Slice> {

  public static Slice EMPTY_SLICE = new Slice();

  private char[] data;
  private int size;
  private int index;

  public Slice() {
    this.data = new char[0];
    this.size = 0;
    this.index = 0;
  }

  public Slice(Slice target) {
    this.data = target.getData();
    this.index = 0;
    this.size = this.data.length;
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

  public Slice substring(int beginIndex, int endIndex) {
    if (beginIndex < 0) {
      throw new IndexOutOfBoundsException("index out of range: " + beginIndex);
    }
    if (endIndex > size) {
      throw new IndexOutOfBoundsException("index out of range: " + endIndex);
    }
    int subLen = endIndex - beginIndex;
    if (subLen < 0) {
      throw new IndexOutOfBoundsException("index out of range: " + subLen);
    }

    this.index = this.index + beginIndex;
    this.size = endIndex - beginIndex;
    return this;
  }

  public Slice concat(char[] data, int offset, int size) {
    char[] buffer = new char[this.size + size];
    System.arraycopy(this.data, index, buffer, 0, this.size);
    System.arraycopy(data, offset, buffer, this.size, size);

    this.data = buffer;
    this.index = 0;
    this.size = buffer.length;
    return this;
  }

  public int getSize() {
    return size;
  }

  public char[] getData() {
    if (data == null) {
      return data;
    }

    return Arrays.copyOfRange(data, index, index + size);
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
    this.data = new char[0];
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
