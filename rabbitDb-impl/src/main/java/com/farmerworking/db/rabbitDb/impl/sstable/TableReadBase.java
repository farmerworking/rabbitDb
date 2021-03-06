package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.*;
import com.farmerworking.db.rabbitDb.impl.ErrorIterator;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.utils.Coding;
import com.farmerworking.db.rabbitDb.impl.utils.Crc32C;
import com.farmerworking.db.rabbitDb.impl.utils.SnappyWrapper;
import org.apache.commons.lang3.tuple.Pair;

public class TableReadBase {
    private static boolean verifyChecksums(ReadOptions readOptions, char[] content, BlockHandle blockHandle) {
        if (readOptions.verifyChecksums()) {
            int actual = Crc32C.mask(Crc32C.value(new String(content, 0, (int) blockHandle.getSize() + 1)));
            int expect = Coding.decodeFixed32(content, (int) blockHandle.getSize() + 1).getRight();

            if (actual != expect) {
                return false;
            }
        }

        return true;
    }

    static Pair<Status, Block> readBlock(RandomAccessFile file, ReadOptions readOptions, String blockHandleContent) {
        BlockHandle blockHandle = new BlockHandle();
        Pair<Status, Integer> pair = blockHandle.decodeFrom(blockHandleContent);
        Status status = pair.getLeft();

        if (status.isOk()) {
            return readBlock(file, readOptions, blockHandle, new SnappyWrapper());
        } else {
            return Pair.of(status, null);
        }
    }

    protected static DBIterator<String, String> readDataBlock(RandomAccessFile file, ReadOptions readOptions, DBComparator comparator, String blockHandleContent) {
        Pair<Status, Block> pair = readBlock(file, readOptions, blockHandleContent);
        if (pair.getLeft().isOk()) {
            return pair.getRight().iterator(comparator);
        } else {
            return new ErrorIterator<>(pair.getLeft());
        }
    }

    protected static DBIterator<String, String> readDataBlock(RandomAccessFile file, ReadOptions readOptions, DBComparator comparator, BlockHandle blockHandle) {
        Pair<Status, Block> pair = readBlock(file, readOptions, blockHandle);
        if (pair.getLeft().isOk()) {
            return pair.getRight().iterator(comparator);
        } else {
            return new ErrorIterator<>(pair.getLeft());
        }
    }


    static Pair<Status, Block> readBlock(RandomAccessFile file, ReadOptions readOptions, BlockHandle blockHandle) {
        return readBlock(file, readOptions, blockHandle, new SnappyWrapper());
    }

    static Pair<Status, Block> readBlock(RandomAccessFile file, ReadOptions readOptions, BlockHandle blockHandle, SnappyWrapper snappyWrapper) {
        Pair<Status, String> readResult = file.read(blockHandle.getOffset(), blockHandle.getSize() + TableBuilder.BLOCK_TRAILER_SIZE);
        Status status = readResult.getLeft();
        Block block = null;

        if (status.isOk()) {
            char[] content = readResult.getRight().toCharArray();

            if (content.length != blockHandle.getSize() + TableBuilder.BLOCK_TRAILER_SIZE) {
                return Pair.of(Status.corruption("truncated block read"), null);
            }

            if (!verifyChecksums(readOptions, content, blockHandle)) {
                status = Status.corruption("block checksum mismatch");
            } else {
                if ((int)content[(int)blockHandle.getSize()] == CompressionType.SNAPPY.persistentId()) {
                    try {
                        block = new Block(snappyWrapper.uncompress(new String(content, 0, (int)blockHandle.getSize()).getBytes("ISO-8859-1")));
                    } catch (Exception e) {
                        status = Status.corruption("corrupted compressed block contents");
                    }
                } else {
                    block = new Block(new String(content, 0, (int)blockHandle.getSize()));
                }
            }
        }
        return Pair.of(status, block);
    }
}
