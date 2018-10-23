package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.ReadOptions;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.ErrorIterator;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class TableIterator implements DBIterator<Slice, Slice> {
    private final DBIterator<Slice, Slice> indexIter;
    private final ReadOptions readOptions;
    private final RandomAccessFile file;
    private final DBComparator comparator;
    private DBIterator<Slice, Slice> dataBlockIter;
    private Status status;
    private String dataBlockHandle;

    public TableIterator(DBIterator<Slice, Slice> indexIter, ReadOptions readOptions, RandomAccessFile file, DBComparator comparator) {
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
    public void seek(Slice key) {
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
    public Slice key() {
        assert (isValid());
        return dataBlockIter.key();
    }

    @Override
    public Slice value() {
        assert (isValid());
        return dataBlockIter.value();
    }

    private void initDataBlock() {
        if (!indexIter.isValid()) {
            setDataBlockIter(null);
        } else {
            Slice blockHandleContent = indexIter.value();

            if (dataBlockIter != null && StringUtils.equals(dataBlockHandle, blockHandleContent.toString())) {
                // dataBlockIter is already constructed with this iterator, so no need to change anything
                return;
            }

            DBIterator<Slice, Slice> iter = readDataBlock(blockHandleContent);
            dataBlockHandle = blockHandleContent.toString();
            setDataBlockIter(iter);
        }
    }

    private DBIterator<Slice, Slice> readDataBlock(Slice blockHandleContent) {
        BlockHandle blockHandle = new BlockHandle();
        Pair<Status, Integer> pair = blockHandle.decodeFrom(blockHandleContent);
        Status status = pair.getLeft();

        if (status.isOk()) {
            Pair<Status, Slice> readResult = file.read(blockHandle.getOffset(), blockHandle.getSize());
            status = readResult.getLeft();

            if (status.isOk()) {
                Block block = new Block(readResult.getRight());
                return block.iterator(this.comparator);
            } else {
                return new ErrorIterator<>(status);
            }
        } else {
            return new ErrorIterator<>(status);
        }
    }

    void setDataBlockIter(DBIterator<Slice, Slice> dataBlockIter) {
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
