package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.ReadOptions;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import com.farmerworking.db.rabbitDb.impl.utils.Crc32C;
import org.apache.commons.lang3.tuple.Pair;

public abstract class TableReadBase {
    private static boolean verifyChecksums(ReadOptions readOptions, char[] content, BlockHandle blockHandle) {
        if (readOptions.verifyChecksums()) {
            int actual = Crc32C.mask(Crc32C.value(new String(content, 0, (int) blockHandle.getSize())));
            int expect = Coding.decodeFixed32(content, (int) blockHandle.getSize()).getRight();

            if (actual != expect) {
                return false;
            }
        }

        return true;
    }

    protected static Pair<Status, Block> readBlock(RandomAccessFile file, ReadOptions readOptions, Slice blockHandleContent) {
        BlockHandle blockHandle = new BlockHandle();
        Pair<Status, Integer> pair = blockHandle.decodeFrom(blockHandleContent);
        Status status = pair.getLeft();

        if (status.isOk()) {
            return readBlock(file, readOptions, blockHandle);
        } else {
            return Pair.of(status, null);
        }
    }

    protected static Pair<Status, Block> readBlock(RandomAccessFile file, ReadOptions readOptions, BlockHandle blockHandle) {
        Pair<Status, Slice> readResult = file.read(blockHandle.getOffset(), blockHandle.getSize() + Coding.FIXED_32_UNIT);
        Status status = readResult.getLeft();
        Block block = null;

        if (status.isOk()) {
            char[] content = readResult.getRight().getData();

            if (!verifyChecksums(readOptions, content, blockHandle)) {
                status = Status.corruption("block checksum mismatch");
            } else {
                block = new Block(new Slice(content, (int)blockHandle.getSize()));
            }
        }
        return Pair.of(status, block);
    }
}
