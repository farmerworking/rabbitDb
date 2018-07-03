Design goals

1. 100% compatible with leveldb database, which means you can try rabbitDb now and shift to rabbitDb if it fits without touching your original database
2. 100% leveldb original unit test passing
3. 100% function coverage of leveldb
4. share the same Level DB API interface like project fusesource/leveldbjni and dain/leveldb

Todo List

Improvement

Compatible Issues

1. encoding compatible (fixed32, fixed64, varint32, varint64)
2. hash compatible
3. crc value compatible
4. bloom filter string representation compatible
