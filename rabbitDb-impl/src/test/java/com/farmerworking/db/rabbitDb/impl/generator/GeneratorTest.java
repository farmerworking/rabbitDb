package com.farmerworking.db.rabbitDb.impl.generator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GeneratorTest {
    @Test
    public void testSequence() {
        long last = SequenceGenerator.last();
        long generate = SequenceGenerator.generate(10);
        assertEquals(last + 1, generate);
        assertEquals(last + 10, SequenceGenerator.last());
    }

    @Test
    public void testFileNumber() throws Exception {
        long last = FileNumberGenerator.last();
        long generate = FileNumberGenerator.generate(10);
        assertEquals(last + 1, generate);
        assertEquals(last + 10, FileNumberGenerator.last());
    }

    @Test
    public void testIsolation() throws Exception {

        long originFileNumberLast = FileNumberGenerator.last();

        long last = SequenceGenerator.last();
        long generate = SequenceGenerator.generate(10);

        assertEquals(last + 1, generate);
        assertEquals(last + 10, SequenceGenerator.last());
        assertEquals(originFileNumberLast, FileNumberGenerator.last());
        assertEquals(originFileNumberLast + 1, FileNumberGenerator.generate(1).longValue());
    }
}