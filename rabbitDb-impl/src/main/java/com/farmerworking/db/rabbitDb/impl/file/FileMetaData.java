package com.farmerworking.db.rabbitDb.impl.file;

import com.farmerworking.db.rabbitDb.impl.memtable.InternalKey;
import lombok.Data;

@Data
public class FileMetaData {
    private long fileNumber;
    private long fileSize;
    private InternalKey minKey;
    private InternalKey maxKey;
}
