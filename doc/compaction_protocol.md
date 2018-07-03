# Compaction

### Triggers

1. db init
2. manual compaction (level, start, end)
3. after a compaction is done (level + 1 may need another compaction)
4. memtable switch
5. every read

### Compaction Types

Ordered by priority:

1. immutable memtable compaction(**execute memtable compaction even in the middle of other type of compaction， since disk compaction may be time costly and memtable compaction may delay write**)
2. manual compaction
3. disk level compaction
4. seek compaction

### Compaction Protocol

1. lock	
2. compact memtable internal
3. pick disk compaction by priority
4. simple move the file up to level + 1 if it's a trivial compaction
5. else do compaction work
6. unlock

### Compact MemTable Internal

1. allocate a file number, write memtable as L0 table file
2. update manifest to record new L0 table file
3. clear immutable memtable
4. delete obsolete file

### Do Compaction Work

1. drop key if a. it's a old key. b. for key delete. c. has more recent insert
2. use valid key to build new table file
3. use a new table file when current table file is big enough(2M) or hit level overlap size (force write to disk storage)
4. update manifest
5. delete obsolete file

### Notes

1. pipeline compaction, no concurrent compaction
2. no compaction during db shutting down
3. trivial compaction --- single file from level, no overlap from level + 1, overlap size of level + 2 is endure (20MB)
4. limit level overlap size to avoid very expensive merge



