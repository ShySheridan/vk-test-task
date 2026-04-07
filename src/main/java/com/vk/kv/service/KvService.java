package com.vk.kv.service;

import com.vk.kv.model.KvRecord;

import java.util.Optional;
import java.util.function.Consumer;

public interface KvService {

    void put(String key, byte[] value);

    /** Returns empty when the key does not exist. */
    Optional<KvRecord> get(String key);

    /** Returns {@code true} if the key existed and was removed. */
    boolean delete(String key);

    /** Delivers records in [keySince, keyTo] to {@code sink} in ascending order. */
    void range(String keySince, String keyTo, Consumer<KvRecord> sink);

    long count();
}
