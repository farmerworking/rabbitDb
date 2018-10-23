package com.farmerworking.db.rabbitDb.impl.utils;

import java.util.Random;

public class TestUtils {
    static char[] TEST_CHARS = {
            '\0', '\1', 'a', 'b', 'c', 'd', 'e', (char) 253, (char) 254, (char) 255
    };

    public static String randomKey(int len) {
        // Make sure to generate a wide variety of characters so we
        // test the boundary conditions for short-key optimizations.
        StringBuilder result = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < len; i++) {
            result.append(TEST_CHARS[random.nextInt(TEST_CHARS.length)]);
        }
        return result.toString();
    }

    public static String randomString(int len) {
        char[] chars = new char[len];
        Random random = new Random();

        char c = ' ';
        for (int i = 0; i < len; i++) {
            chars[i] = (char) (((int)c) + random.nextInt(95));
        }

        return new String(chars);
    }
}
