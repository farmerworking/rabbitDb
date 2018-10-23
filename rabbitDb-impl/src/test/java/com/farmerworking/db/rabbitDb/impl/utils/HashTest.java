package com.farmerworking.db.rabbitDb.impl.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class HashTest {
    @Test
    public void testSignedUnsignedIssue() {
        char[] data0 = {};
        char[] data1 = {0x62};
        char[] data2 = {0xc3, 0x97};
        char[] data3 = {0xe2, 0x99, 0xa5};
        char[] data4 = {0xe1, 0x80, 0xb9, 0x32};
        char[] data5 = {
                0x01, 0xc0, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x04, 0x00,
                0x00, 0x00, 0x00, 0x14,
                0x00, 0x00, 0x00, 0x18,
                0x28, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
        };

        assertEquals(Integer.compareUnsigned(Hash.hash(data0, 0xbc9f1d34), 0xbc9f1d34), 0);
        assertEquals(Integer.compareUnsigned(Hash.hash(data1, 0xbc9f1d34), 0xef1345c4), 0);
        assertEquals(Integer.compareUnsigned(Hash.hash(data2, 0xbc9f1d34), 0x5b663814), 0);
        assertEquals(Integer.compareUnsigned(Hash.hash(data3, 0xbc9f1d34), 0x323c078f), 0);
        assertEquals(Integer.compareUnsigned(Hash.hash(data4, 0xbc9f1d34), 0xed21633a), 0);
        assertEquals(Integer.compareUnsigned(Hash.hash(data5, 0x12345678), 0xf333dabb), 0);
    }
}