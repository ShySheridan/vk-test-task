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
import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
public class TarantoolKvRepository implements KvRepository {

    private static final int    BATCH_SIZE = 1_000;
    private static final String SPACE_NAME = "KV";

    private final TarantoolBoxClient client;
    private final TarantoolBoxSpace  space;

    public TarantoolKvRepository(TarantoolBoxClient client) {
        this.client = client;
        this.space  = client.space(SPACE_NAME);
    }

    @Override
    public void put(KvRecord record) {
        try {
            space.replace(Arrays.asList(record.key(), record.value())).get();
            log.debug("put key={}", record.key());
        } catch (Exception e) {
            log.error("put failed: key={}", record.key(), e);
            throw new StorageException("put", e);
        }
    }

    @Override
    public Optional<KvRecord> get(String key) {
        try {
            SelectResponse<List<Tuple<List<?>>>> resp =
                    space.select(Arrays.asList(key)).get();
            List<Tuple<List<?>>> rows = resp.get();

            if (rows == null || rows.isEmpty()) {
                log.debug("get key={} → not found", key);
                return Optional.empty();
            }
            log.debug("get key={} → found", key);
            return Optional.of(toRecord(rows.get(0).get()));
        } catch (Exception e) {
            log.error("get failed: key={}", key, e);
            throw new StorageException("get", e);
        }
    }

    @Override
    public boolean delete(String key) {
        try {
            Tuple<List<?>> deleted = space.delete(Arrays.asList(key)).get();
            boolean existed = deleted != null
                    && deleted.get() != null
                    && !deleted.get().isEmpty();
            log.debug("delete key={} → existed={}", key, existed);
            return existed;
        } catch (Exception e) {
            log.error("delete failed: key={}", key, e);
            throw new StorageException("delete", e);
        }
    }

    /**
     * Paginated range scan over the TREE primary index.
     * Semantics: [keySince, keyTo] — both endpoints inclusive.
     *   • first batch  — GE from keySince (includes keySince)
     *   • next batches — GT from last seen key (strictly after it)
     */
    @Override
    public void range(String keySince, String keyTo, Consumer<KvRecord> sink) {
        try {
            String  currentKey = keySince;
            boolean first      = true;
            long    delivered  = 0;

            outer:
            while (true) {
                SelectOptions opts = SelectOptions.builder()
                        .withIterator(first ? BoxIterator.GE : BoxIterator.GT)
                        .withLimit(BATCH_SIZE)
                        .build();
                first = false;

                SelectResponse<List<Tuple<List<?>>>> resp =
                        space.select(Arrays.asList(currentKey), opts).get();
                List<Tuple<List<?>>> rows = resp.get();

                if (rows == null || rows.isEmpty()) break;

                for (Tuple<List<?>> tuple : rows) {
                    List<?> fields = tuple.get();
                    String  key    = (String) fields.get(0);

                    if (key.compareTo(keyTo) > 0) break outer;

                    sink.accept(toRecord(fields));
                    currentKey = key;
                    delivered++;
                }

                if (rows.size() < BATCH_SIZE) break;
            }
            log.debug("range [{}, {}] → {} records delivered", keySince, keyTo, delivered);
        } catch (Exception e) {
            log.error("range failed: keySince={} keyTo={}", keySince, keyTo, e);
            throw new StorageException("range", e);
        }
    }

    /** Uses box.space.KV:len() — O(1) for memtx engine. */
    @Override
    public long count() {
        try {
            var result = client.eval("return box.space.KV:len()").get();
            long count = ((Number) result.get().get(0)).longValue();
            log.debug("count → {}", count);
            return count;
        } catch (Exception e) {
            log.error("count failed", e);
            throw new StorageException("count", e);
        }
    }


    private KvRecord toRecord(List<?> fields) {
        return new KvRecord((String) fields.get(0), (byte[]) fields.get(1));
    }
}
