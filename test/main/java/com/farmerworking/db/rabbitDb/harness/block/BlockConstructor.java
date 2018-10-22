package com.farmerworking.db.rabbitDb.harness.block;

import com.farmerworking.db.rabbitDb.DBIterator;
import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;
import com.farmerworking.db.rabbitDb.sstable.Block;
import com.farmerworking.db.rabbitDb.sstable.BlockBuilder;
import com.farmerworking.db.rabbitDb.harness.Constructor;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;

import java.util.Vector;

public class BlockConstructor extends Constructor {
    private Block block;

    public BlockConstructor(DBComparator comparator) {
        super(comparator);
    }

    @Override
    public Status finishImpl(Options options, Vector<String> keys) {
        BlockBuilder blockBuilder = new BlockBuilder(options.blockRestartInterval(), options.comparator());

        for(String key : keys) {
            blockBuilder.add(new Slice(key), new Slice(data.get(key)));
        }

        Slice blockContent = blockBuilder.finish();
        this.block = new Block(blockContent);
        return Status.ok();
    }

    @Override
    public DBIterator newIterator() {
        return block.iterator(this.comparator);
    }
}
