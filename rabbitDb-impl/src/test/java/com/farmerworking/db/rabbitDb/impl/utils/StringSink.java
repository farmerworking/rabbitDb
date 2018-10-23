package com.farmerworking.db.rabbitDb.impl.utils;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.file.WritableFile;

public class StringSink implements WritableFile {
    private StringBuilder content = new StringBuilder();

    public String getContent() {
        return content.toString();
    }

    @Override
    public Status append(Slice data) {
        content.append(data.toString());
        return Status.ok();
    }

    @Override
    public Status close() {
        return Status.ok();
    }

    @Override
    public Status flush() {
        return Status.ok();
    }

    @Override
    public Status sync() {
        return Status.ok();
    }
}
