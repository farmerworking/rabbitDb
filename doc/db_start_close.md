# DB Start and Close

### DB start

1. compression support
2. database directory init
3. comparator init
4. memtable
5. compaction thread
6. table cache
7. lock database dir
8. version recover
9. log file recover (file number larger or equal to the log file number recorded in manifest) --- apply write to memtable, update last sequence, flush memtable to disk
10. open transaction log
11. delete obsolete file
12. schedule compaction



### DB close

1. compaction thread wait and destroy
2. version close
3. log close
4. table cache close
5. db lock release