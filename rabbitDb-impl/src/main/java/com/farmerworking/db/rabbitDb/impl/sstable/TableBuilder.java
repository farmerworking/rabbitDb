package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.CompressionType;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.Status;
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

    // filter
    private FilterBlockBuilder filterBlockBuilder;

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

        // filter
        if (options.filterPolicy() != null) {
            this.filterBlockBuilder = new FilterBlockBuilder(options.filterPolicy());
            this.filterBlockBuilder.startBlock(fileOffset);
        }
    }

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: finish(), abandon() have not been called
    public void add(String key, String value) {
        assert !this.closed;
        if (this.status.isNotOk()) { return; }
        if (numEntries > 0) assert this.options.comparator().compare(key, lastKey) > 0;

        if (pendingIndexEntry) {
            pendingIndex(key);
        }

        this.lastKey = key;
        dataBlockBuilder.add(key, value);
        numEntries ++;
        if (this.filterBlockBuilder != null) {
            this.filterBlockBuilder.addKey(this.lastKey);
        }

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

        BlockHandle filterBlockHandle = new BlockHandle(), metaIndexHandle = new BlockHandle();

        // filter
        if (this.status.isOk() && this.filterBlockBuilder != null) {
            String filterContent = this.filterBlockBuilder.finish();
            writeRawBlock(filterContent, CompressionType.NONE.persistentId(), filterBlockHandle);
        }

        if (this.status.isOk()) {
            BlockBuilder metaIndexBuilder = new BlockBuilder(this.options.blockRestartInterval(), this.options.comparator());
            if (this.filterBlockBuilder != null) {
                String encode = filterBlockHandle.encode();
                metaIndexBuilder.add("filter." + this.options.filterPolicy().name(), encode);
            }

            writeBlock(metaIndexBuilder, metaIndexHandle);
        }

        // index
        BlockHandle indexHandle = new BlockHandle();
        if (this.status.isOk()) {
            writeBlock(indexBlockBuilder, indexHandle);
        }

        // footer
        if (this.status.isOk()) {
            Footer footer = new Footer();
            footer.setIndexHandle(indexHandle);
            footer.setMetaIndexHandle(metaIndexHandle);

            String encode = footer.encode();

            this.status = file.append(encode);
            if (status.isOk()) {
                fileOffset += encode.length();
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
            if (filterBlockBuilder != null) {
                filterBlockBuilder.startBlock(fileOffset);
            }
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

    private void pendingIndex(String key) {
        assert this.dataBlockBuilder.isEmpty();
        String indexKey;
        if (key == null) {
            indexKey = this.options.comparator().findShortSuccessor(lastKey);
        } else {
            indexKey = this.options.comparator().findShortestSeparator(lastKey, key);
        }

        String encode = pendingBlockHandle.encode();

        indexBlockBuilder.add(indexKey, encode);
        pendingIndexEntry = false;
    }

    private void writeBlock(BlockBuilder builder, BlockHandle handle) {
        String blockContent = builder.finish();
        int type = CompressionType.NONE.persistentId();

        if (this.options.compressionType().equals(CompressionType.SNAPPY)) {
            String compressContent = null;

            try {
                compressContent = new String(snappyWrapper.compress(blockContent), "ISO-8859-1");
            } catch (Exception e) {
                // fallback to no compression
            }

            if (compressContent != null && (test ||
                    compressContent.length() < blockContent.length() - (blockContent.length() / 8))) {
                blockContent = compressContent;
                type = CompressionType.SNAPPY.persistentId();
            }
        }

        writeRawBlock(blockContent, type, handle);
        builder.reset();
    }

    private void writeRawBlock(String blockContent, int type, BlockHandle blockHandle) {
        blockHandle.setOffset(fileOffset);
        blockHandle.setSize(blockContent.length());
        status = file.append(blockContent);

        if (status.isOk()) {
            StringBuilder builder = new StringBuilder();
            builder.append((char) type);

            int crc = Crc32C.value(blockContent);
            crc = Crc32C.extend(crc, builder.toString());
            int mask = Crc32C.mask(crc);

            Coding.putFixed32(builder, mask);
            status = file.append(builder.toString());

            if (status.isOk()) {
                fileOffset += blockContent.length() + BLOCK_TRAILER_SIZE;
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
