package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.*;
import com.farmerworking.db.rabbitDb.api.Slice;
import com.farmerworking.db.rabbitDb.impl.SequenceGenerator;
import com.farmerworking.db.rabbitDb.impl.SnapshotImpl;
import com.farmerworking.db.rabbitDb.impl.harness.Constructor;
import com.farmerworking.db.rabbitDb.impl.utils.StringSink;
import com.farmerworking.db.rabbitDb.impl.utils.StringSource;
import org.apache.commons.lang3.tuple.Pair;

import static org.junit.Assert.*;

import java.util.Vector;

public class TableConstructor extends Constructor {
    private Table table;

    public TableConstructor(DBComparator comparator) {
        super(comparator);
    }

    @Override
    public Status finishImpl(Options options, Vector<String> keys) {
        options.paranoidChecks(true);
        StringSink file = new StringSink();
        TableBuilder builder = new TableBuilder(options, file);
        builder.setTest(true);

        for(String key : keys) {
            builder.add(new Slice(key), new Slice(data.get(key)));
            assertTrue(builder.status().isOk());
        }
        Status status = builder.finish();
        assertTrue(status.isOk());

        assertEquals(file.getContent().length(), builder.fileSize());
        StringSource stringSource = new StringSource(file.getContent());
        Pair<Status, Table> pair = Table.open(options, stringSource, (int) builder.fileSize());
        assertTrue(pair.getLeft().isOk());
        table = pair.getRight();
        return Status.ok();
    }

    @Override
    public DBIterator newIterator() {
        ReadOptions readOptions = new ReadOptions();
        readOptions.verifyChecksums(true);
        return this.table.iterator(readOptions);
    }

    @Override
    public boolean suppportGet() {
        return true;
    }

    @Override
    public Slice get(Slice key) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.verifyChecksums(true);
        readOptions.snapshot(new SnapshotImpl(SequenceGenerator.last()));
        Pair<Status, Slice> pair = table.get(readOptions, key);
        assert pair.getLeft().isOk();
        return pair.getRight();
    }
}
