package com.vk.kv.model;

/**
 * Domain object representing a single KV entry.
 * {@code value} is nullable (stored as varbinary IS NULL in Tarantool).
 */
public record KvRecord(String key, byte[] value) {}
