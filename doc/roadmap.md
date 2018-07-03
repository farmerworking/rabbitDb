# Roadmap

### Phase 1 --- key-value read write

1. write memory
2. read memory

methods to implement:

```
byte[] get(byte[] key) throws DBException;

DBIterator iterator();

void put(byte[] key, byte[] value) throws DBException;
void delete(byte[] key) throws DBException;
void write(WriteBatch updates) throws DBException;

WriteBatch createWriteBatch();
```



options to support:

```
Comparator comparator
long writeBufferSize // byte unit
```





### Phase 2 --- Snapshot Support



### Phase 3 --- capacity

1. flush memory to disk --- level0 database
2. read from memory first, disk second



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

