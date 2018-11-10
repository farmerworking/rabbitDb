package com.farmerworking.db.rabbitDb.impl.file.posix;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;

public class PosixRandomAccessFile implements RandomAccessFile{
    private final java.io.RandomAccessFile randomAccessFile;

    public PosixRandomAccessFile(java.io.RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    @Override
    public Pair<Status, String> read(int offset, int n) {
        try {
            byte[] bytes = new byte[n];
            randomAccessFile.read(bytes, offset, n);
            return Pair.of(Status.ok(), new String(bytes, "ISO-8859-1"));
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }
}
