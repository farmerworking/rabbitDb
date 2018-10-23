package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.file.WritableFile;

public class TableBuilder {
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

    private void writeBlock(BlockBuilder blockBuilder, BlockHandle blockHandle) {
        Slice blockContent = blockBuilder.finish();
        blockHandle.setOffset(fileOffset);
        blockHandle.setSize(blockContent.getSize());
        status = file.append(blockContent);

        if (status.isOk()) {
            fileOffset += blockContent.getSize();
        }
        blockBuilder.reset();
    }

    // only use for test
    void setStatus(Status status) {
        this.status = status;
    }
}
