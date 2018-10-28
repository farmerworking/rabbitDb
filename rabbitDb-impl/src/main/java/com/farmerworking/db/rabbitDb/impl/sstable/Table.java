package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.ReadOptions;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.utils.SnappyWrapper;
import org.apache.commons.lang3.tuple.Pair;

public class Table extends TableReadBase {
    private final RandomAccessFile file;
    private final Options options;
    private final Block indexBlock;

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

        Pair<Status, Slice> pair = file.read(size - Footer.ENCODE_LENGTH, Footer.ENCODE_LENGTH);
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
        return Pair.of(status, new Table(options, indexBlock, file));
    }

    public TableIterator iterator(ReadOptions readOptions) {
        DBIterator<Slice, Slice> iter = this.indexBlock.iterator(this.options.comparator());
        return new TableIterator(iter, readOptions, file, this.options.comparator());
    }
}
