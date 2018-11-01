package com.farmerworking.db.rabbitDb.impl.utils;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.api.Slice;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import org.apache.commons.lang3.tuple.Pair;

public class StringSource implements RandomAccessFile {
    private final String content;

    public StringSource(String content) {
        this.content = content;
    }

    @Override
    public Pair<Status, Slice> read(long offset, long size) {
        char[] dst = new char[(int) size];
        content.getChars((int)offset, (int)(offset + size), dst, 0);
        return Pair.of(Status.ok(), new Slice(dst));
    }
}