

# Write(batch write)

`Put` and `Delete` is just convenient function for single key insert/delete. Both function underlay use write



write protocol:

1. lock
2. make room for write
3. acquire sequence number for each write(query and set lastSequence)
4. write log
5. update memtable
6. unlock



### make room for write(both disk and memory is involved):

##### case 1: memtable is full and immutable memtable is present

##### case 2: L0 disk apce is full (hit hard limit)

wait until background compaction work complete

##### case 3: L0 disk space is tight(files number hit soft limit)

delay each write 1ms, reduce write latency variance, hand over CPU for compaction work (release lock in the meantime)

##### case 4: memtable is full and no immutable memtable

switch to a new memtable, trigger memtable compaction 

notes:

1. close current log and create a new one with larger file number before memtable switch —— log file can be deleted when corresponding immutable memtable is compacted to disk, reduce recovery workload

##### case 5：both disk and memory has room

nothing



### Log Record Structure

sequenceBegin + updateSize + Tag(insert/delete) + key length + key + (option: value length + value)



