package com.farmerworking.db.rabbitDb.impl.harness.memtable;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.Slice;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.harness.Constructor;
import com.farmerworking.db.rabbitDb.impl.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.impl.memtable.Memtable;
import com.farmerworking.db.rabbitDb.impl.memtable.ValueType;

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
