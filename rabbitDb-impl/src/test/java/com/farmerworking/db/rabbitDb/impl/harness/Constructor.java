package com.farmerworking.db.rabbitDb.impl.harness;

import com.farmerworking.db.rabbitDb.api.DBIterator;
import com.farmerworking.db.rabbitDb.api.Status;
import org.apache.commons.lang3.tuple.Pair;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class Constructor {
    protected ConcurrentSkipListMap<String, String> data;
    protected DBComparator comparator;

    public Constructor(DBComparator comparator) {
        this.data = new ConcurrentSkipListMap<>(new ConstructorComparator(comparator));
        this.comparator = comparator;
    }

    public void add(String key, String value) {
        data.put(key, value);
    }

    public Pair<Vector<String>, ConcurrentSkipListMap<String, String>> finish(Options options) {
        Vector<String> keys = new Vector<>();

        Iterator<String> iter = data.navigableKeySet().iterator();
        while(iter.hasNext()) {
            keys.add(iter.next());
        }

        Status status = finishImpl(options, keys);
        assert status.isOk();

        return Pair.of(keys, data);
    }

    public abstract Status finishImpl(Options options, Vector<String> keys) ;

    public abstract DBIterator newIterator() ;

    public Map<String, String> data() {
        return data;
    }
}
