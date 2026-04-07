package com.vk.kv.service;

import com.vk.kv.model.KvRecord;
import com.vk.kv.repository.KvRepository;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
public class KvServiceImpl implements KvService {

    private final KvRepository repository;

    public KvServiceImpl(KvRepository repository) {
        this.repository = repository;
    }

    @Override
    public void put(String key, byte[] value) {
        repository.put(new KvRecord(key, value));
    }

    @Override
    public Optional<KvRecord> get(String key) {
        return repository.get(key);
    }

    @Override
    public boolean delete(String key) {
        return repository.delete(key);
    }

    @Override
    public void range(String keySince, String keyTo, Consumer<KvRecord> sink) {
        repository.range(keySince, keyTo, sink);
    }

    @Override
    public long count() {
        return repository.count();
    }
}
