package com.farmerworking.db.rabbitDb.impl.file.posix;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.file.WritableFile;

import java.io.FileOutputStream;
import java.io.IOException;

public class PosixWritableFile implements WritableFile {
    private final FileOutputStream fileOutputStream;

    public PosixWritableFile(FileOutputStream fileOutputStream) {
        this.fileOutputStream = fileOutputStream;
    }

    @Override
    public Status append(String data) {
        try {
            fileOutputStream.write(data.getBytes("ISO-8859-1"));
            return Status.ok();
        } catch (IOException e) {
            return Status.iOError(e.getMessage());
        }
    }

    @Override
    public Status close() {
        try {
            fileOutputStream.close();
            return Status.ok();
        } catch (IOException e) {
            return Status.iOError(e.getMessage());
        }
    }

    @Override
    public Status flush() {
        try {
            fileOutputStream.flush();
            return Status.ok();
        } catch (IOException e) {
            return Status.iOError(e.getMessage());
        }
    }

    @Override
    public Status sync() {
        try {
            fileOutputStream.getFD().sync();
            return Status.ok();
        } catch (IOException e) {
            return Status.iOError(e.getMessage());
        }
    }
}
