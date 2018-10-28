package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.CompressionType;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.file.WritableFile;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import com.farmerworking.db.rabbitDb.impl.utils.Crc32C;
import com.farmerworking.db.rabbitDb.impl.utils.SnappyWrapper;

public class TableBuilder {
    public static final int BLOCK_TRAILER_SIZE = 1 + Coding.FIXED_32_UNIT;

    private final Options options;
    private final WritableFile file;

    private BlockBuilder dataBlockBuilder;

    // index
    private BlockBuilder indexBlockBuilder;
    private boolean pendingIndexEntry;
    private BlockHandle pendingBlockHandle;
    private String lastKey;

    private Status status;
    private int numEntries;
    private int fileOffset;
    private boolean closed;
    private SnappyWrapper snappyWrapper = new SnappyWrapper();
    private boolean test = false;

    public TableBuilder(Options options, WritableFile file) {
        this.options = options;
        this.file = file;

        this.dataBlockBuilder = new BlockBuilder(options.blockRestartInterval(), options.comparator());

        // index
        this.indexBlockBuilder = new BlockBuilder(1, options.comparator());
        this.pendingIndexEntry = false;
        this.lastKey = null;
        this.pendingBlockHandle = new BlockHandle();

        this.status = Status.ok();
        this.numEntries = 0;
        this.fileOffset = 0;
        this.closed = false;
    }

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: finish(), abandon() have not been called
    public void add(Slice key, Slice value) {
        assert !this.closed;
        if (this.status.isNotOk()) { return; }
        if (numEntries > 0) assert this.options.comparator().compare(key.getData(), lastKey.toCharArray()) > 0;

        if (pendingIndexEntry) {
            pendingIndex(key);
        }

        this.lastKey = key.toString();
        dataBlockBuilder.add(key, value);
        numEntries ++;

        if (dataBlockBuilder.currentSizeEstimate() >= options.blockSize()) {
            flush();
        }
    }

    public Status status() {
        return status;
    }

    // finish building the table.  Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: finish(), abandon() have not been called
    public Status finish() {
        flush();
        assert !this.closed;
        this.closed = true;

        if (this.status.isNotOk()) { return status; }

        if (this.status.isOk() && this.pendingIndexEntry) {
            pendingIndex(null);
        }

        // index
        BlockHandle metaIndexHandle = new BlockHandle(fileOffset, 0);
        BlockHandle indexHandle = new BlockHandle();
        if (this.status.isOk()) {
            writeBlock(indexBlockBuilder, indexHandle);
        }

        // footer
        if (this.status.isOk()) {
            Footer footer = new Footer();
            footer.setIndexHandle(indexHandle);
            footer.setMetaIndexHandle(metaIndexHandle);

            StringBuilder builder = new StringBuilder();
            footer.encodeTo(builder);

            String footerContent = builder.toString();
            this.status = file.append(new Slice(footerContent));
            if (status.isOk()) {
                fileOffset += footerContent.length();
            }
        }

        return this.status;
    }

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.  Most clients should not need to use this method.
    // REQUIRES: finish(), abandon() have not been called
    public void flush() {
        assert !this.closed;
        if (this.status.isNotOk()) { return; }
        if (this.dataBlockBuilder.isEmpty()) return;
        assert !this.pendingIndexEntry;

        writeBlock(dataBlockBuilder, pendingBlockHandle);
        if (status.isOk()) {
            pendingIndexEntry = true;
            status = file.flush();
        }
    }

    // Number of calls to Add() so far.
    public long numEntries() {
        return numEntries;
    }


    // Indicate that the contents of this builder should be abandoned.  Stops
    // using the file passed to the constructor after this function returns.
    // If the caller is not going to call finish(), it must call abandon()
    // before destroying this builder.
    // REQUIRES: finish(), abandon() have not been called
    public void abandon() {
        assert !this.closed;
        if (this.status.isNotOk()) { return; }

        this.closed = true;
    }

    // Size of the file generated so far.  If invoked after a successful
    // finish() call, returns the size of the final generated file.
    public long fileSize() {
        return fileOffset;
    }

    private void pendingIndex(Slice key) {
        assert this.dataBlockBuilder.isEmpty();
        String indexKey;
        if (key == null) {
            indexKey = new String(this.options.comparator().findShortSuccessor(lastKey.toCharArray()));
        } else {
            indexKey = new String(this.options.comparator().findShortestSeparator(lastKey.toCharArray(), key.getData()));
        }

        StringBuilder builder = new StringBuilder();
        pendingBlockHandle.encodeTo(builder);

        indexBlockBuilder.add(new Slice(indexKey), new Slice(builder.toString()));
        pendingIndexEntry = false;
    }

    private void writeBlock(BlockBuilder builder, BlockHandle handle) {
        Slice blockContent = builder.finish();
        int type = CompressionType.NONE.persistentId();

        if (this.options.compressionType().equals(CompressionType.SNAPPY)) {
            Slice compressContent = null;

            try {
                compressContent = new Slice(new String(snappyWrapper.compress(blockContent.toString()), "ISO-8859-1"));
            } catch (Exception e) {
                // fallback to no compression
            }

            if (compressContent != null && (test ||
                    compressContent.getSize() < blockContent.getSize() - (blockContent.getSize() / 8))) {
                blockContent = compressContent;
                type = CompressionType.SNAPPY.persistentId();
            }
        }

        writeRawBlock(blockContent, type, handle);
        builder.reset();
    }

    private void writeRawBlock(Slice blockContent, int type, BlockHandle blockHandle) {
        blockHandle.setOffset(fileOffset);
        blockHandle.setSize(blockContent.getSize());
        status = file.append(blockContent);

        if (status.isOk()) {
            StringBuilder builder = new StringBuilder();
            builder.append((char) type);

            int crc = Crc32C.value(blockContent.toString());
            crc = Crc32C.extend(crc, builder.toString());
            int mask = Crc32C.mask(crc);

            Coding.putFixed32(builder, mask);
            status = file.append(new Slice(builder.toString()));

            if (status.isOk()) {
                fileOffset += blockContent.getSize() + BLOCK_TRAILER_SIZE;
            }
        }
    }

    // only use for test
    void setTest(boolean isTest) {
        this.test = isTest;
    }

    void setSnappyWrapper(SnappyWrapper snappyWrapper) {
        this.snappyWrapper = snappyWrapper;
    }

    void setStatus(Status status) {
        this.status = status;
    }
}
