package com.farmerworking.db.rabbitDb.impl.utils;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import org.apache.commons.lang3.tuple.Pair;

public class ErrorRandomAccessFile implements RandomAccessFile {
    @Override
    public Pair<Status, Slice> read(long offset, long n) {
        return Pair.of(Status.iOError("read error"), null);
    }
}
