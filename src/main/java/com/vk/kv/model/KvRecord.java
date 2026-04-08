package com.vk.kv.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * Domain object representing a single KV entry.
 * {@code value} is nullable (stored as varbinary IS NULL in Tarantool).
 */
public record KvRecord(String key, byte[] value) {

    public KvRecord {
        Objects.requireNonNull(key, "key must not be null");
        value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }
}