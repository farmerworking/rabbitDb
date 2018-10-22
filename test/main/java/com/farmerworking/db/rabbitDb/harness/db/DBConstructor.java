package com.farmerworking.db.rabbitDb.harness.db;

import com.farmerworking.db.rabbitDb.DBIterator;
import com.farmerworking.db.rabbitDb.DbImpl;
import com.farmerworking.db.rabbitDb.Slice;
import com.farmerworking.db.rabbitDb.Status;
import com.farmerworking.db.rabbitDb.harness.Constructor;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;

import java.util.Vector;

public class DBConstructor extends Constructor {
    private DbImpl db;

    public DBConstructor(DBComparator comparator) {
        super(comparator);
    }

    @Override
    public Status finishImpl(Options options, Vector<String> keys) {
        this.db = new DbImpl(options);

        for(String key : keys) {
            db.put(new Slice(key), new Slice(data.get(key)));
        }

        return Status.ok();
    }

    @Override
    public DBIterator newIterator() {
        return db.iterator();
    }
}
