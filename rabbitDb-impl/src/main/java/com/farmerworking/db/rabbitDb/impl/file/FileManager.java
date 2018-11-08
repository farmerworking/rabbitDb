package com.farmerworking.db.rabbitDb.impl.file;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.impl.memtable.InternalEntryComparator;
import com.farmerworking.db.rabbitDb.impl.memtable.InternalKey;

import java.util.ArrayList;
import java.util.List;

public class FileManager {
    private InternalEntryComparator comparator;
    private List<FileMetaData> fileIndex;

    public FileManager(DBComparator comparator) {
        this.fileIndex = new ArrayList<>();
        this.comparator = new InternalEntryComparator(comparator);
    }

    public synchronized void addTableFile(FileMetaData fileMetaData) {
        this.fileIndex.add(fileMetaData);
    }

    public synchronized List<FileMetaData> filesToLookup(InternalKey key) {
        List<FileMetaData> result = new ArrayList<>();

        for (FileMetaData item : fileIndex) {
            if (comparator.compare(item.getMinKey(), key) >= 0) {
                continue;
            }

            if (comparator.compare(item.getMaxKey(), key) <= 0) {
                continue;
            }

            result.add(item);
        }

        return result;
    }
}
