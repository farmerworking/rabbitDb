package com.farmerworking.db.rabbitDb.impl.file.posix;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.file.SequentialFile;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.io.IOException;

public class PosixSequentialFile implements SequentialFile {
    private final FileInputStream fileInputStream;

    public PosixSequentialFile(FileInputStream fileInputStream) {
        this.fileInputStream = fileInputStream;
    }

    @Override
    public Pair<Status, String> read(int n) {
        try {
            byte[] bytes = new byte[n];
            this.fileInputStream.read(bytes);
            return Pair.of(Status.ok(), new String(bytes, "ISO-8859-1"));
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }

    @Override
    public Status skip(long n) {
        try {
            this.fileInputStream.skip(n);
            return Status.ok();
        } catch (IOException e) {
            return Status.iOError(e.getMessage());
        }
    }
}
