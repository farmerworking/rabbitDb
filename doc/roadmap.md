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

1. encoding compatible —— hash, crc32c, fix length, variant length
2. compression —— snappy, common prefix of key compression
3. error detection —— crc32c, file footer mark with magic number
4. performance —— between files using file index, between blocks using block index, within single block using filter first, binary search second

implementation steps:

1. encoding
2. crc32c
3. snappy compression
4. status
5. data block builder/reader —— common prefix of key compression
6. filter data block builder/reader
7. bloom filter implementation
8. sstable footer
9. basic table builder —— data block + index block
10. add crc32c checksum to block for data integrity
11. support block compression
12. support filter meta block
13. global file number generator
14. file meta and L0 file manager
15. env interface and implementation
16. file name util
17. block and do memory table compaction
18. introduce immutable memory table and async memory table compaction. only block when last compaction not finished

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

