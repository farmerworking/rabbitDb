package com.farmerworking.db.rabbitDb;

import com.farmerworking.db.rabbitDb.memtable.InternalKey;
import com.farmerworking.db.rabbitDb.memtable.Memtable;
import com.farmerworking.db.rabbitDb.writebatch.WriteBatchImpl;
import com.farmerworking.db.rabbitDb.writebatch.WriteBatchMemtableIterateHandler;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;

public class DbImpl {

  private final Memtable memtable;
  private final Options options;
  private final DBComparator comparator;

  public DbImpl(Options options) {
    this.options = options;
    if (options.comparator() == null) {
      this.comparator = ByteWiseComparator.getInstance();
    } else {
      this.comparator = options.comparator();
    }
    this.memtable = new Memtable(this.comparator);
  }

  public Slice get(Slice key) throws DBException {
    return get(key, getDefaultReadOptions());
  }

  public Snapshot put(Slice key, Slice value) throws DBException {
    WriteBatchImpl writeBatch = createWriteBatch();
    writeBatch.put(key, value);
    return write(writeBatch);
  }

  public Snapshot delete(Slice key) throws DBException {
    WriteBatchImpl writeBatch = createWriteBatch();
    writeBatch.delete(key);
    return write(writeBatch);
  }

  public DBIteratorImpl iterator() {
    return iterator(getDefaultReadOptions());
  }

  public DBIteratorImpl iterator(ReadOptions options) {
    long sequence = ((SnapshotImpl) options.snapshot()).getSequence();
    return new DBIteratorImpl(memtable.iterator(), sequence, this.comparator);
  }

  public Snapshot write(WriteBatchImpl writeBatch) throws DBException {
    writeBatch.setSequence(SequenceGenerator.generate(writeBatch.getCount()));

    long memoryUsageAfterUpdate = writeBatch.approximateSize() + memtable.approximateMemoryUsage();
    if (memoryUsageAfterUpdate > this.options.writeBufferSize()) {
      throw new DBException("memory has run out");
    } else {
      writeBatch.iterate(new WriteBatchMemtableIterateHandler(memtable));
      return new SnapshotImpl(writeBatch.getSequence() + writeBatch.getCount() - 1);
    }
  }

  public Slice get(Slice key, ReadOptions options) throws DBException {
    long sequence = ((SnapshotImpl) options.snapshot()).getSequence();
    return memtable.get(new InternalKey(key, sequence, null));
  }

  public Snapshot getSnapshot() {
    return new SnapshotImpl(SequenceGenerator.last());
  }

  public WriteBatchImpl createWriteBatch() {
    return new WriteBatchImpl();
  }

  private ReadOptions getDefaultReadOptions() {
    ReadOptions options = new ReadOptions();
    options.snapshot(getSnapshot());
    return options;
  }
}
