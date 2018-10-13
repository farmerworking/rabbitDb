package com.farmerworking.db.rabbitDb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class Crc32CTest {

    @Test
    public void test() {
        //// From rfc3720 section B.4.
        char[] data = new char[32];

        for (int i = 0; i < 32; i++) {
            data[i] = (char) 0;
        }
        assertEquals(0x8a9136aa, Crc32C.value(new String(data)));

        for (int i = 0; i < 32; i++) {
            data[i] = (char) 0xff;
        }
        assertEquals(0x62a8ab43, Crc32C.value(new String(data)));

        for (int i = 0; i < 32; i++) {
            data[i] = (char) i;
        }
        assertEquals(0x46dd794e, Crc32C.value(new String(data)));

        for (int i = 0; i < 32; i++) {
            data[i] = (char) (31 - i);
        }
        assertEquals(0x113fdb5c, Crc32C.value(new String(data)));

        data = new char[]{
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
                0x00, 0x00, 0x00, 0x00
        };
        assertEquals(0xd9963a56, Crc32C.value(new String(data)));
    }

    @Test
    public void test1() {
        assertNotEquals(Crc32C.value("a"), Crc32C.value("foo"));
    }

    @Test
    public void test2() {
        assertEquals(Crc32C.value("hello world"), Crc32C.extend(Crc32C.value("hello"), " world"));
        assertEquals(Crc32C.value("hello world"), Crc32C.extend(Crc32C.value("hello "), "world"));
        assertEquals(Crc32C.value("hello world"), Crc32C.extend(Crc32C.value("hello w"), "orld"));
        assertEquals(Crc32C.value("hello world"), Crc32C.extend(Crc32C.value("hello wo"), "rld"));

    }

    @Test
    public void test3() {
        assertNotEquals(Crc32C.value("hello world"), Crc32C.extend(Crc32C.value("hello "), "1world"));
    }

    @Test
    public void test4() {
        int crc = Crc32C.value("foo");
        assertNotEquals(crc, Crc32C.mask(crc));
        assertNotEquals(crc, Crc32C.mask(Crc32C.mask(crc)));
        assertEquals(crc, Crc32C.unmask(Crc32C.mask(crc)));
        assertEquals(crc, Crc32C.unmask(Crc32C.unmask(Crc32C.mask(Crc32C.mask(crc)))));
    }

}