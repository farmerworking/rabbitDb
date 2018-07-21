package com.farmerworking.db.rabbitDb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SequenceGeneratorTest {

  @Test
  public void test() {
    long last = SequenceGenerator.last();
    long generate = SequenceGenerator.generate(10);
    assertEquals(last + 1, generate);
    assertEquals(last + 10, SequenceGenerator.last());
  }
}