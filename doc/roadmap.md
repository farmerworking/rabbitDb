# Roadmap

### Phase 1 --- key-value memory read/write, support snapshot

1. write memory
2. read memory

methods to implement:

```
byte[] get(byte[] key) throws DBException;
byte[] get(byte[] key, ReadOptions options) throws DBException

DBIterator iterator();
DBIterator iterator(ReadOptions options);

Snapshot put(byte[] key, byte[] value) throws DBException;
Snapshot delete(byte[] key) throws DBException;
Snapshot write(WriteBatch updates) throws DBException;

WriteBatch createWriteBatch();
Snapshot getSnapshot();
```

options to support:

```
Comparator comparator
long writeBufferSize // byte unit
```

ReadOptions to support:

```
snapshot
```

Limits:

1. can only store limited data since memory is the only storage

More details:

1. attach an increasing sequence number for every record
2. use a special typed record to represent delete operation without physical deletion
3. base on above design design, we can support snapshot(only records whose sequence number <= sequence number user provided can be seen)
4. the data structure should first sort by key and second sort by sequence number to achieve efficient read/write operation. SkipList is suitable for this situation and chosen. 

### Phase 2 --- capacity

1. flush memory to disk when it's full --- level0 database
2. read from memory first, disk second

design issues:

1. encoding compatible —— hash, crc, fix length, variant length
2. compression —— snappy, common prefix of key compression
3. error detection —— CRC, file mark with magic number
4. index
5. filter —— bloom filter

implementation steps:

1. encoding
2. block —— snappy compression, CRC
3. sstable —— data block —— common prefix of key compression
4. sstable —— index block
5. sstable —— meta index block
6. sstable —— meta block —— bloom filter
7. sstable —— footer —— file mark with magic number
8. sstable

### Phase 3 --- persistent

1. log first before write memory
2. db recover from log during startup

### Phase 4 --- reduce duplicate, sort,

before:

1. duplicate data
2. joint
3. unsorted

before performance: all table index scan + 1/4 valid tables content scan (almost 1/2 table will be valid and almost need scan 1/2 to find result)



after:

1. no duplicate
2. disjoint
3. sorted

after performance: original performance on a much smaller scale + 1/2 table index scan + only half of single table content scan



1. L0 level disk compaction to L1 level

