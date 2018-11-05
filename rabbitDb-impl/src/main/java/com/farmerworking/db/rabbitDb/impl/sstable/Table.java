package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.ReadOptions;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.utils.SnappyWrapper;
import org.apache.commons.lang3.tuple.Pair;

public class Table extends TableReadBase {
    private RandomAccessFile file;
    private final Options options;
    private final Block indexBlock;
    FilterBlockReader filter;

    public Table(Options options, Block indexBlock, RandomAccessFile file) {
        this.options = options;
        this.indexBlock =  indexBlock;
        this.file = file;
    }

    public static Pair<Status, Table> open(Options options, RandomAccessFile file, int size) {
        Status status;
        if (size < Footer.ENCODE_LENGTH) {
            status = Status.corruption("file is too short to be an sstable");
            return Pair.of(status, null);
        }

        Pair<Status, String> pair = file.read(size - Footer.ENCODE_LENGTH, Footer.ENCODE_LENGTH);
        status = pair.getLeft();

        if (status.isNotOk()) return Pair.of(status, null);
        Footer footer = new Footer();
        status = footer.decodeFrom(pair.getRight());

        if (status.isNotOk()) return Pair.of(status, null);

        ReadOptions readOptions = new ReadOptions();
        if (options.paranoidChecks()) {
            readOptions.verifyChecksums(true);
        } else {
            readOptions.verifyChecksums(false);
        }

        Pair<Status, Block> readResult = readBlock(file, readOptions, footer.getIndexHandle(), new SnappyWrapper());
        status = readResult.getLeft();
        if (status.isNotOk()) return Pair.of(status, null);
        Block indexBlock = readResult.getRight();
        Table table = new Table(options, indexBlock, file);
        table.readMeta(footer);
        return Pair.of(status, table);
    }

    public TableIterator iterator(ReadOptions readOptions) {
        DBIterator<String, String> iter = this.indexBlock.iterator(this.options.comparator());
        return new TableIterator(iter, readOptions, file, this.options.comparator());
    }

    public Pair<Status, String> get(ReadOptions readOptions, String key) {
        DBIterator<String, String> iter = this.indexBlock.iterator(this.options.comparator());
        iter.seek(key);

        if (iter.isValid()) {
            String blockHandleContent = iter.value();
            BlockHandle blockHandle = new BlockHandle();
            Pair<Status, Integer> pair = blockHandle.decodeFrom(blockHandleContent);

            if (pair.getLeft().isNotOk()) {
                return Pair.of(pair.getLeft(), null);
            } else {
                if (this.filter != null) {
                    boolean filterResult = this.filter.keyMayMatch(blockHandle.getOffset(), key);

                    if (!filterResult) {
                        return Pair.of(Status.ok(), null);
                    }
                }
                DBIterator<String, String> blockIter = readDataBlock(file, readOptions, this.options.comparator(), blockHandle);
                blockIter.seek(key);

                if (blockIter.isValid() && blockIter.key().equals(key)) {
                    return Pair.of(blockIter.getStatus(), blockIter.value());
                } else {
                    return Pair.of(blockIter.getStatus(), null);
                }
            }
        } else {
            return Pair.of(iter.getStatus(), null);
        }
    }

    void readMeta(Footer footer) {
        if (options.filterPolicy() == null) {
            return; // do not need any metadata
        }

        if (footer.getMetaIndexHandle().getSize() == 0) {
            return; // empty;
        }

        ReadOptions readOptions = new ReadOptions();
        readOptions.verifyChecksums(this.options.verifyChecksums() || this.options.paranoidChecks());

        Pair<Status, Block> pair = readBlock(file, readOptions, footer.getMetaIndexHandle());
        if (pair.getLeft().isNotOk()) {
            // Do not propagate errors since meta info is not needed for operation
            return;
        }

        Block metaIndexBlock = pair.getRight();
        DBIterator<String, String> iter = metaIndexBlock.iterator(ByteWiseComparator.getInstance());
        String key = "filter." + this.options.filterPolicy().name();
        iter.seek(key);
        if (iter.isValid() && iter.key().equals(key)) {
            readFilter(iter.value());
        }
    }

    void readFilter(String value) {
        BlockHandle filterBlockHandle = new BlockHandle();
        Pair<Status, Integer> pair = filterBlockHandle.decodeFrom(value);
        if (pair.getLeft().isNotOk()) {
            return;
        }

        ReadOptions readOptions = new ReadOptions();
        readOptions.verifyChecksums(this.options.verifyChecksums() || this.options.paranoidChecks());

        Pair<Status, Block> readResult = readBlock(file, readOptions, filterBlockHandle);
        if (readResult.getLeft().isNotOk()) {
            return;
        }

        this.filter = new FilterBlockReader(this.options.filterPolicy(), readResult.getRight().getBlockContent());

    }

    // only for unit test
    void setFile(RandomAccessFile file) {
        this.file = file;
    }
}
