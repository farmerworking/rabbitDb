package com.farmerworking.db.rabbitDb.impl.harness.block;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.harness.Constructor;
import com.farmerworking.db.rabbitDb.impl.sstable.Block;
import com.farmerworking.db.rabbitDb.impl.sstable.BlockBuilder;
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
