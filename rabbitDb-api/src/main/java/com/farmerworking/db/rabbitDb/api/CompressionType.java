package com.farmerworking.db.rabbitDb.api;

/**
 * Created by John on 18/10/23.
 */
public enum CompressionType {
    NONE(0x00),
    SNAPPY(0x01);

    public static CompressionType getCompressionTypeByPersistentId(int persistentId)
    {
        for (CompressionType compressionType : CompressionType.values()) {
            if (compressionType.persistentId == persistentId) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException("Unknown persistentId " + persistentId);
    }

    private final int persistentId;

    CompressionType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int persistentId()
    {
        return persistentId;
    }
}
