package com.farmerworking.db.rabbitDb.impl.writebatch;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class WriteBatchImpl {

    private List<WriteBatchItem> updateList = new ArrayList<>();
    private
    @Getter
    @Setter
    Long sequence;

    public WriteBatchImpl put(String key, String value) {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        updateList.add(new WriteBatchItem(key, value));
        return this;
    }

    public WriteBatchImpl delete(String key) {
        requireNonNull(key, "key is null");
        updateList.add(new WriteBatchItem(key));
        return this;
    }

    public int getCount() {
        return this.updateList.size();
    }

    public void clear() {
        updateList.clear();
        sequence = null;
    }

    public int approximateSize() {
        return updateList.stream().mapToInt(
                item -> item.isDelete() ?
                        item.getKey().length() :
                        item.getKey().length() + item.getValue().length()).sum();
    }

    public void iterate(WriteBatchIterateHandler handler) {
        assert this.sequence != null;

        handler.setSequence(this.sequence);
        updateList.forEach(item -> {
            if (item.isDelete()) {
                handler.delete(item.getKey());
            } else {
                handler.put(item.getKey(), item.getValue());
            }
        });
    }

    public void append(WriteBatchImpl b2) {
        this.updateList.addAll(b2.updateList);
    }

    public void close() {
    }
}
