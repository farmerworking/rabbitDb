package com.farmerworking.db.rabbitDb.harness.memtable;

import com.farmerworking.db.rabbitDb.DBIterator;
import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;
import com.farmerworking.db.rabbitDb.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.memtable.Memtable;
import com.farmerworking.db.rabbitDb.memtable.ValueType;
import com.farmerworking.db.rabbitDb.harness.Constructor;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;

import java.util.Vector;

public class MemTableConstructor extends Constructor {
    private Memtable memtable;

    public MemTableConstructor(DBComparator comparator) {
        super(comparator);
    }

    @Override
    public Status finishImpl(Options options, Vector<String> keys) {
        memtable = new Memtable(options.comparator());

        int seq = 1;
        for(String key : keys) {
            memtable.add(new InternalKey(new Slice(key), seq, ValueType.VALUE), new Slice(data.get(key)));
            seq ++;
        }
        return Status.ok();
    }

    @Override
    public DBIterator newIterator() {
        return new KeyConvertingIterator(memtable.iterator());
    }
}
