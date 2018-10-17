package com.farmerworking.db.rabbitDb.file;

import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;

/**
 * Created by John on 18/10/17.
 */
// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
public interface WritableFile {
    Status append(Slice data);

    Status close();

    Status flush();

    Status sync();
}
