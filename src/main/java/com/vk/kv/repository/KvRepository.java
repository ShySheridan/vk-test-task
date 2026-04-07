package com.vk.kv.repository;

import com.vk.kv.model.KvRecord;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Storage abstraction for KV operations.
 * Implementations throw {@link StorageException} on infrastructure errors.
 */
public interface KvRepository {

    void put(KvRecord record);

    Optional<KvRecord> get(String key);

    /** Returns {@code true} if the key existed and was removed. */
    boolean delete(String key);

    /**
     * Streams records in [keySince, keyTo] (both inclusive) to {@code sink}
     * in ascending key order, iterating in batches.
     */
    void range(String keySince, String keyTo, Consumer<KvRecord> sink);

    long count();
}
