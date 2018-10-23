package com.farmerworking.db.rabbitDb.api;

import java.util.Comparator;

/**
 * Created by John on 18/10/23.
 */
public interface DBComparator extends Comparator<char[]>{
    String name();

    /**
     * If {@code start < limit}, returns a short key in [start,limit).
     * Simple comparator implementations should return start unchanged,
     */
    char[] findShortestSeparator(char[] start, char[] limit);

    String findShortestSeparator(String start, String limit);

    /**
     * returns a 'short key' where the 'short key' is greater than or equal to key.
     * Simple comparator implementations should return key unchanged,
     */
    char[] findShortSuccessor(char[] key);

    String findShortSuccessor(String key);

    int compare(String o1, String o2);
}
