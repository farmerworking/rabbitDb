package com.farmerworking.db.rabbitDb.impl.file;

import com.farmerworking.db.rabbitDb.api.Slice;
import com.farmerworking.db.rabbitDb.api.Status;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by John on 18/10/17.
 */
// A file abstraction for reading sequentially through a file
public interface SequentialFile {
    // Read up to "n" bytes from the file.
    // If an error was encountered, returns a non-OK status.
    //
    // REQUIRES: External synchronization
    Pair<Status, Slice> read(int n);

    // Skip "n" bytes from the file. This is guaranteed to be no
    // slower that reading the same data, but may be faster.
    //
    // If end of file is reached, skipping will stop at the end of the
    // file, and Skip will return OK.
    //
    // REQUIRES: External synchronization
    Status skip(long n);
}
