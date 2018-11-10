package com.farmerworking.db.rabbitDb.impl.file;

import com.farmerworking.db.rabbitDb.api.Status;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by John on 18/11/10.
 */
public interface Env {
    Pair<Status, WritableFile> newWritableFile(String filename) ;

    Pair<Status, WritableFile> newAppendableFile(String filename) ;

    Pair<Status, RandomAccessFile> newRandomAccessFile(String filename);

    Pair<Status, SequentialFile> newSequentialFile(String filename);

    Pair<Status, String> getTestDirectory();

    Pair<Status, Boolean> isFileExists(String filename);

    Pair<Status, Boolean> delete(String filename);
}
