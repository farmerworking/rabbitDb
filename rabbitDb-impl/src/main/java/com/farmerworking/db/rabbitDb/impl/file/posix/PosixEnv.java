package com.farmerworking.db.rabbitDb.impl.file.posix;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.file.Env;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.file.SequentialFile;
import com.farmerworking.db.rabbitDb.impl.file.WritableFile;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PosixEnv implements Env {
    @Override
    public Pair<Status, WritableFile> newWritableFile(String filename) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filename, false);
            return Pair.of(Status.ok(), new PosixWritableFile(fileOutputStream));
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }

    @Override
    public Pair<Status, WritableFile> newAppendableFile(String filename) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filename, true);
            return Pair.of(Status.ok(), new PosixWritableFile(fileOutputStream));
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }

    @Override
    public Pair<Status, RandomAccessFile> newRandomAccessFile(String filename) {
        try {
            java.io.RandomAccessFile randomAccessFile = new java.io.RandomAccessFile(filename, "r");
            return Pair.of(Status.ok(), new PosixRandomAccessFile(randomAccessFile));
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }

    @Override
    public Pair<Status, SequentialFile> newSequentialFile(String filename) {
        try {
            FileInputStream fileInputStream = new FileInputStream(filename);
            return Pair.of(Status.ok(), new PosixSequentialFile(fileInputStream));
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }

    public Pair<Status, String> getTestDirectory() {
        String directory = String.format("/tmp/leveldbtest-%s", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        try {
            Path path = Paths.get(directory);
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
            return Pair.of(Status.ok(), path.toString());
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }

    public Pair<Status, Boolean> isFileExists(String filename) {
        try {
            return Pair.of(Status.ok(), Files.exists(Paths.get(filename)));
        } catch (Exception e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }

    public Pair<Status, Boolean> delete(String filename) {
        try {
            return Pair.of(Status.ok(), Files.deleteIfExists(Paths.get(filename)));
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }
}
