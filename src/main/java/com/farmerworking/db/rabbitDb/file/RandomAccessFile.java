package com.farmerworking.db.rabbitDb.file;

import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by John on 18/10/17.
 */
// A file abstraction for randomly reading the contents of a file.
public interface RandomAccessFile {
    // Read up to "n" bytes from the file starting at "offset".
    // If an error was encountered, returns a non-OK status.
    //
    // Safe for concurrent use by multiple threads.
    Pair<Status, Slice> read(long offset, long n);
}
