package com.farmerworking.db.rabbitDb.impl.harness.db;

import com.farmerworking.db.rabbitDb.api.DBComparator;
import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.impl.DbImpl;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.harness.Constructor;

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
