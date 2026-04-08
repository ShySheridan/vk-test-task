package com.vk.kv.repository;

import com.vk.kv.model.KvRecord;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@Slf4j
public final class TarantoolKvRepository implements KvRepository {

    private static final int BATCH_SIZE = 1_000;
    private static final String SPACE_NAME = "KV";
    private static final String COUNT_QUERY = "return box.space." + SPACE_NAME + ":len()";

    private static final int KEY_FIELD_INDEX = 0;
    private static final int VALUE_FIELD_INDEX = 1;
    private static final int RECORD_FIELD_COUNT = 2;

    private final TarantoolBoxClient client;
    private final TarantoolBoxSpace space;

    public TarantoolKvRepository(TarantoolBoxClient client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.space = this.client.space(SPACE_NAME);
    }

    @Override
    public void put(KvRecord record) {
        Objects.requireNonNull(record, "record must not be null");

        try {
            space.replace(Arrays.asList(record.key(), record.value())).get();
            log.debug("Put succeeded for key={}", record.key());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Put interrupted for key={}", record.key(), e);
            throw new StorageException("put", e);
        } catch (ExecutionException | RuntimeException e) {
            log.error("Put failed for key={}", record.key(), e);
            throw new StorageException("put", e);
        }
    }

    @Override
    public Optional<KvRecord> get(String key) {
        Objects.requireNonNull(key, "key must not be null");

        try {
            SelectResponse<List<Tuple<List<?>>>> response = space.select(List.of(key)).get();
            List<Tuple<List<?>>> rows = response.get();

            if (rows == null || rows.isEmpty()) {
                log.debug("Get returned no record for key={}", key);
                return Optional.empty();
            }

            KvRecord record = toRecord(rows.get(0).get());
            log.debug("Get succeeded for key={}", key);
            return Optional.of(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Get interrupted for key={}", key, e);
            throw new StorageException("get", e);
        } catch (ExecutionException | RuntimeException e) {
            log.error("Get failed for key={}", key, e);
            throw new StorageException("get", e);
        }
    }

    @Override
    public boolean delete(String key) {
        Objects.requireNonNull(key, "key must not be null");

        try {
            Tuple<List<?>> deletedTuple = space.delete(List.of(key)).get();
            boolean existed = deletedTuple != null
                    && deletedTuple.get() != null
                    && !deletedTuple.get().isEmpty();

            log.debug("Delete completed for key={}, existed={}", key, existed);
            return existed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Delete interrupted for key={}", key, e);
            throw new StorageException("delete", e);
        } catch (ExecutionException | RuntimeException e) {
            log.error("Delete failed for key={}", key, e);
            throw new StorageException("delete", e);
        }
    }

    @Override
    public void range(String keySince, String keyTo, Consumer<KvRecord> sink) {
        Objects.requireNonNull(keySince, "keySince must not be null");
        Objects.requireNonNull(keyTo, "keyTo must not be null");
        Objects.requireNonNull(sink, "sink must not be null");

        try {
            String currentKey = keySince;
            boolean firstBatch = true;
            long delivered = 0;

            outer:
            while (true) {
                SelectOptions options = SelectOptions.builder()
                        .withIterator(firstBatch ? BoxIterator.GE : BoxIterator.GT)
                        .withLimit(BATCH_SIZE)
                        .build();
                firstBatch = false;

                SelectResponse<List<Tuple<List<?>>>> response =
                        space.select(List.of(currentKey), options).get();
                List<Tuple<List<?>>> rows = response.get();

                if (rows == null || rows.isEmpty()) {
                    break;
                }

                for (Tuple<List<?>> tuple : rows) {
                    KvRecord record = toRecord(tuple.get());

                    if (record.key().compareTo(keyTo) > 0) {
                        break outer;
                    }

                    sink.accept(record);
                    currentKey = record.key();
                    delivered++;
                }

                if (rows.size() < BATCH_SIZE) {
                    break;
                }
            }

            log.debug(
                    "Range completed for keySince={}, keyTo={}, delivered={}",
                    keySince,
                    keyTo,
                    delivered
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Range interrupted for keySince={}, keyTo={}", keySince, keyTo, e);
            throw new StorageException("range", e);
        } catch (ExecutionException | RuntimeException e) {
            log.error("Range failed for keySince={}, keyTo={}", keySince, keyTo, e);
            throw new StorageException("range", e);
        }
    }

    @Override
    public long count() {
        try {
            var result = client.eval(COUNT_QUERY).get();
            Object rawCount = result.get().get(0);

            if (!(rawCount instanceof Number number)) {
                throw new IllegalStateException("Unexpected count result type: " + rawCount);
            }

            long count = number.longValue();
            log.debug("Count completed for space={}, count={}", SPACE_NAME, count);
            return count;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Count interrupted for space={}", SPACE_NAME, e);
            throw new StorageException("count", e);
        } catch (ExecutionException | RuntimeException e) {
            log.error("Count failed for space={}", SPACE_NAME, e);
            throw new StorageException("count", e);
        }
    }

    private KvRecord toRecord(List<?> fields) {
        Objects.requireNonNull(fields, "fields must not be null");

        if (fields.size() < RECORD_FIELD_COUNT) {
            throw new IllegalStateException("Tuple has fewer fields than expected: " + fields.size());
        }

        Object rawKey = fields.get(KEY_FIELD_INDEX);
        Object rawValue = fields.get(VALUE_FIELD_INDEX);

        if (!(rawKey instanceof String key)) {
            throw new IllegalStateException("Tuple field 0 is not a String: " + rawKey);
        }

        if (rawValue != null && !(rawValue instanceof byte[])) {
            throw new IllegalStateException("Tuple field 1 is not byte[] or null: " + rawValue);
        }

        return new KvRecord(key, (byte[]) rawValue);
    }
}