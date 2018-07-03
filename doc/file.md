# File

### Component 1 --- Current

##### usage

point current using manifest file

##### write protocol

1. write content to a tmp file
2. rename the tmp file to current once all write success

##### content

MANIFEST-*\n

##### filename

CURRENT



### Component  2 --- Manifest

##### data structure

![无标题绘图.png](https://i.loli.net/2018/07/01/5b38b830ee600.png)

The whole content is encoded as a log entry value

##### usage

1. comparator name --- verify user comparator consistent
2. log number --- verify, log recover (valid log file), obsolete file deletion
3. pre log number --- old implementation, now is no longer used, backwards compatibility
3. next file --- global file number generator
4. last sequence --- last sequence used, generate a sequence number per record, internal select newest data
5. compact point --- next compaction file selection
6. deleted file --- used for version recovery, record file deletion, 
7. new file --- used for version recovery, record file insertion, contain file data index

##### filename

MANIFEST-*



### Component 3 --- sstable

##### data structure

![无标题绘图 (3).png](https://i.loli.net/2018/06/22/5b2ca40ae8611.png)

##### malformed block deal strategy

1. malformed index block --- return Corruption status when open table
2. malformed meta index block or malformed meta block --- ignore and continue since meta info is not needed for operation 
3. malformed data block --- reflect by the status of table iterator
4. foot has no crc to verify. it only contains pointer to blocks in sstable file and rely on block's crc to verify data integrity

##### features

1. support options change in runtime

##### test cases

filter block test case:

1. Empty
2. SingleChunk
3. MultiChunk

bloom filter test case:

1. EmptyFilter
2. SmallFilter
3. VaryingLengthFilter (check false positive rate, check filter string length)

table test case:

1. Empty
2. ZeroRestartPointsInBlock
3. SimpleEmptyKey
4. SimpleSingle
5. SimpleMulti
6. SimpleSpecialKey
7. RandomizedKeyValue
8. ApproximateOffsetOfPlain
9. ApproximateOffsetOfCompressed

##### filename

*.sst



### Component 4 --- Log

##### data structure

![无标题绘图 (2).png](https://i.loli.net/2018/06/20/5b29c77f2422b.png)

##### type enum

```
Full
First
Middle
Last
```

##### physical log record type

```
FullRecord
FirstRecord
MiddleRecord
LastRecord
BadRecord
EofRecord
UnknownRecord
```

##### LL(1) for logical log record

```
LogicalRecord    = FullRecord
                   EofRecord
                   BadRecord LogicalRecord
                   MiddleRecord LogicalRecord
                   LastRecord LogicalRecord
                   UnknownRecord LogicalRecord
                   FirstRecord FragmentRecord

FragmentRecord   = LastRecord
                   EofRecord
                   MiddleRecord FragmentRecord
                   BadRecord LogicalRecord
                   FirstRecord LogicalRecord
                   FullRecord LogicalRecord
                   Unknown LogicalRecord
```

##### semantic

```
LogicalRecord                 = FullRecord                          FullRecord
                                EofRecord                           EofRecord
                                BadRecord LogicalRecord             LogicalRecord
                                MiddleRecord LogicalRecord          LogicalRecord
                                LastRecord LogicalRecord            LogicalRecord
                                UnknownRecord LogicalRecord         LogicalRecord
                                FirstRecord FragmentRecord          FragmentRecord(FirstRecord)

FragmentRecord(FirstRecord)   = LastRecord                          FirstRecord + LastRecord
                                EofRecord                           EofRecord
                                MiddleRecord FragmentRecord         FragmentRecord(FirstRecord + MiddleRecord)
                                BadRecord LogicalRecord             LogicalRecord
                                FirstRecord LogicalRecord           LogicalRecord(FirstRecord)
                                FullRecord LogicalRecord            LogicalRecord(FullRecord)
                                Unknown LogicalRecord               LogicalRecord
```

##### malformed log file deal strategy

general strategy:

1. skip malformed log record, report, and continue since log record is not designed to be repairable so that there is not much can be done with malformed log record
2. due to malformed trailing of log file, we can not direct append to reuse the old log file when system restart

For malformed trailing in log file，2 ways to do:

1. ignore --- use a new log file when system restart
2. overwrite --- traverse log file until the end and append at the start offset of malformed trailing

##### test cases

normal test case:

1. EmptyRead
2. WriteRead --- including empty string
3. WriteReadFragment
4. WriteReadManyBlock
5. MarginalTrailerFollowEmptyString
6. MarginalTrailerFollowNonEmptyString
7. ShortTrailer
8. AlignedEof
9. OpenForAppend
10. RandomLengthWriteRead
11. SkipInTheMiddleOfMultiRecord
12. InitialOffsetReadRecord

error path test case:

1. ReadError
2. SkipError
3. TruncatedTrailingRecordIsIgnoredReturnEof --- caused by system crash in the middle of write
4. BadLengthInTheMiddleOfFile
5. BadLengthAtEnd
6. ChecksumMismatchCrcChanged
7. ChecksumMismatchTypeContentChanged
8. BadRecordType
9. UnexpectedMiddleType
10. UnexpectedLastType
11. UnexpectedFullType
12. UnexpectedFirstType
13. MissingLastTypeRecord
14. PartialLast
15. ErrorJoinsRecord
16. InitialOffsetPastEnd

##### filename

*.log

### Component 5 --- Lock
no content. file exists only for process hold a global lock

### Component 6 --- Info Log
process runtime log

### Component 7 --- Tmp

##### usage

write content to tmp file and rename tmp file to other type file once all writes success

##### filename

*.dbtmp










