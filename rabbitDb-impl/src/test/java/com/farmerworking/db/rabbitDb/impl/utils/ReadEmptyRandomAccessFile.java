package com.farmerworking.db.rabbitDb.impl.utils;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import org.apache.commons.lang3.tuple.Pair;

public class ReadEmptyRandomAccessFile implements RandomAccessFile {
    @Override
    public Pair<Status, String> read(long offset, long n) {
        return Pair.of(Status.ok(), "");
    }
}
