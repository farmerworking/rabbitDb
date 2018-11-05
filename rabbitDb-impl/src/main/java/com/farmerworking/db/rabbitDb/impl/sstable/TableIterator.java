package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.ReadOptions;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import org.apache.commons.lang3.StringUtils;

public class TableIterator extends TableReadBase implements DBIterator<String, String> {
    private final DBIterator<String, String> indexIter;
    private final ReadOptions readOptions;
    private final RandomAccessFile file;
    private final DBComparator comparator;
    private DBIterator<String, String> dataBlockIter;
    private Status status;
    private String dataBlockHandle;

    public TableIterator(DBIterator<String, String> indexIter, ReadOptions readOptions, RandomAccessFile file, DBComparator comparator) {
        this.indexIter = indexIter;
        this.readOptions = readOptions;
        this.file = file;
        this.comparator = comparator;
        this.status = Status.ok();
        this.dataBlockHandle = null;
    }

    @Override
    public Status getStatus() {
        if (indexIter.getStatus().isNotOk()) {
            return indexIter.getStatus();
        } else if (dataBlockIter != null && dataBlockIter.getStatus().isNotOk()) {
            return dataBlockIter.getStatus();
        }
        return this.status;
    }

    @Override
    public boolean isValid() {
        return dataBlockIter != null && dataBlockIter.isValid();
    }

    @Override
    public void next() {
        assert (isValid());
        dataBlockIter.next();

        if (!dataBlockIter.isValid()) {
            indexIter.next();
            initDataBlock();

            if (dataBlockIter != null) {
                dataBlockIter.seekToFirst();
            }
        }
    }

    @Override
    public void prev() {
        assert (isValid());
        dataBlockIter.prev();

        if (!dataBlockIter.isValid()) {
            indexIter.prev();
            initDataBlock();

            if (dataBlockIter != null) {
                dataBlockIter.seekToLast();
            }
        }
    }

    @Override
    public void seekToFirst() {
        indexIter.seekToFirst();
        initDataBlock();

        if (dataBlockIter != null) {
            dataBlockIter.seekToFirst();
        }
    }

    @Override
    public void seekToLast() {
        indexIter.seekToLast();
        initDataBlock();
        if (dataBlockIter != null) {
            dataBlockIter.seekToLast();
        }
    }

    @Override
    public void seek(String key) {
        indexIter.seek(key);
        initDataBlock();
        if (dataBlockIter != null) {
            dataBlockIter.seek(key);
            if (!dataBlockIter.isValid()) {
                indexIter.next();
                initDataBlock();
                if (dataBlockIter != null) {
                    dataBlockIter.seekToFirst();
                }
            }
        }
    }

    @Override
    public String key() {
        assert (isValid());
        return dataBlockIter.key();
    }

    @Override
    public String value() {
        assert (isValid());
        return dataBlockIter.value();
    }

    private void initDataBlock() {
        if (!indexIter.isValid()) {
            setDataBlockIter(null);
        } else {
            String blockHandleContent = indexIter.value();

            if (dataBlockIter != null && StringUtils.equals(dataBlockHandle, blockHandleContent)) {
                // dataBlockIter is already constructed with this iterator, so no need to change anything
                return;
            }

            DBIterator<String, String> iter = readDataBlock(file, readOptions, comparator, blockHandleContent);
            dataBlockHandle = blockHandleContent;
            setDataBlockIter(iter);
        }
    }

    void setDataBlockIter(DBIterator<String, String> dataBlockIter) {
        if (this.dataBlockIter != null) {
            saveError(this.dataBlockIter.getStatus());
        }
        this.dataBlockIter = dataBlockIter;
    }

    private void saveError(Status status) {
        if (this.status.isOk() && status.isNotOk()) {
            this.status = status;
        }
    }
}
