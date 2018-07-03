### Snapshot

a leveldb snapshot object only contains a sequence number which can be used in following read operations

```
Snapshots provide consistent read-only views over the entire state of the
key-value store.  `ReadOptions::snapshot` may be non-NULL to indicate that a
read should operate on a particular version of the DB state. If
`ReadOptions::snapshot` is NULL, the read will operate on an implicit snapshot
of the current state
```

notes:

1. any record whose sequence number is larger than the snapshot's sequence number will be ignored
2. an implicit snapshot will use the currently latest sequence number