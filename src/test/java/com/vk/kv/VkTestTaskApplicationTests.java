package com.vk.kv;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.vk.kv.config.TarantoolClientConfig;
import com.vk.kv.grpc.KvGrpcService;
import com.vk.kv.proto.*;
import com.vk.kv.repository.TarantoolKvRepository;
import com.vk.kv.service.KvServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.Server;
import io.tarantool.client.box.TarantoolBoxClient;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests – requires Tarantool running locally (docker-compose up).
 * Uses an in-process gRPC channel so no real port is needed.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class VkTestTaskApplicationTests {

    private static TarantoolBoxClient tarantoolClient;
    private static Server             grpcServer;
    private static ManagedChannel     channel;
    private static KvServiceGrpc.KvServiceBlockingStub stub;

    @BeforeAll
    static void setUp() throws Exception {
        tarantoolClient = TarantoolClientConfig.buildClient();
        KvGrpcService grpcService = new KvGrpcService(
                new KvServiceImpl(new TarantoolKvRepository(tarantoolClient)));

        String serverName = InProcessServerBuilder.generateName();
        grpcServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(grpcService)
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        stub = KvServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() throws Exception {
        channel.shutdownNow();
        grpcServer.shutdownNow();
        tarantoolClient.close();
    }

    // ── put & get ────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void putAndGet_regularValue() {
        byte[] payload = "hello".getBytes();

        stub.put(PutRequest.newBuilder()
                .setKey("test-key")
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFrom(payload)))
                .build());

        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("test-key").build());
        assertEquals("test-key", resp.getKey());
        assertEquals(NullableBytes.KindCase.DATA, resp.getValue().getKindCase());
        assertArrayEquals(payload, resp.getValue().getData().toByteArray());
    }

    @Test
    @Order(2)
    void put_overwritesExistingKey() {
        byte[] v1 = "v1".getBytes();
        byte[] v2 = "v2".getBytes();

        stub.put(PutRequest.newBuilder()
                .setKey("overwrite-key")
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFrom(v1)))
                .build());

        stub.put(PutRequest.newBuilder()
                .setKey("overwrite-key")
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFrom(v2)))
                .build());

        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("overwrite-key").build());
        assertArrayEquals(v2, resp.getValue().getData().toByteArray());
    }

    @Test
    @Order(3)
    void putAndGet_nullValue() {
        stub.put(PutRequest.newBuilder()
                .setKey("null-key")
                .setValue(NullableBytes.newBuilder().setNullValue(Empty.getDefaultInstance()))
                .build());

        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("null-key").build());
        assertEquals("null-key", resp.getKey());
        assertEquals(NullableBytes.KindCase.NULL_VALUE, resp.getValue().getKindCase());
    }

    @Test
    @Order(4)
    void get_notFound_returnsNotFoundStatus() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
                () -> stub.get(GetRequest.newBuilder().setKey("no-such-key").build()));
        assertEquals(io.grpc.Status.Code.NOT_FOUND, ex.getStatus().getCode());
    }

    // ── delete ───────────────────────────────────────────────────────────────

    @Test
    @Order(5)
    void delete_existingKey_returnsTrue() {
        stub.put(PutRequest.newBuilder()
                .setKey("del-key")
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("x")))
                .build());

        DeleteResponse resp = stub.delete(DeleteRequest.newBuilder().setKey("del-key").build());
        assertTrue(resp.getDeleted());
    }

    @Test
    @Order(6)
    void delete_missingKey_returnsFalse() {
        DeleteResponse resp = stub.delete(
                DeleteRequest.newBuilder().setKey("does-not-exist").build());
        assertFalse(resp.getDeleted());
    }

    // ── range ────────────────────────────────────────────────────────────────

    // range semantics: [key_since, key_to] — both endpoints inclusive

    @Test
    @Order(7)
    void range_inclusiveBothEnds() {
        for (String k : List.of("range-a", "range-b", "range-c", "range-d")) {
            stub.put(PutRequest.newBuilder()
                    .setKey(k)
                    .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8(k)))
                    .build());
        }

        Iterator<RangeEntry> it = stub.range(
                RangeRequest.newBuilder().setKeySince("range-a").setKeyTo("range-c").build());

        List<String> keys = new ArrayList<>();
        it.forEachRemaining(e -> keys.add(e.getKey()));

        // both boundaries are included
        assertTrue(keys.contains("range-a"), "key_since must be included");
        assertTrue(keys.contains("range-c"), "key_to must be included");
        // key beyond key_to is excluded
        assertFalse(keys.contains("range-d"), "key beyond key_to must be excluded");
        // result is sorted ascending
        assertEquals(keys, keys.stream().sorted().toList(), "keys must be in ascending order");
    }

    @Test
    @Order(14)
    void range_equalBoundaries_returnsSingleKey() {
        stub.put(PutRequest.newBuilder()
                .setKey("range-single")
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("v")))
                .build());

        Iterator<RangeEntry> it = stub.range(RangeRequest.newBuilder()
                .setKeySince("range-single").setKeyTo("range-single").build());

        List<String> keys = new ArrayList<>();
        it.forEachRemaining(e -> keys.add(e.getKey()));

        assertEquals(List.of("range-single"), keys);
    }

    // ── validation ───────────────────────────────────────────────────────────

    @Test
    @Order(9)
    void put_emptyKey_returnsInvalidArgument() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
                () -> stub.put(PutRequest.newBuilder()
                        .setKey("")
                        .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("v")))
                        .build()));
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    }

    @Test
    @Order(10)
    void get_emptyKey_returnsInvalidArgument() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
                () -> stub.get(GetRequest.newBuilder().setKey("").build()));
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    }

    @Test
    @Order(11)
    void delete_emptyKey_returnsInvalidArgument() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
                () -> stub.delete(DeleteRequest.newBuilder().setKey("").build()));
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    }

    @Test
    @Order(12)
    void range_emptyKeySince_returnsInvalidArgument() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
                () -> stub.range(RangeRequest.newBuilder().setKeySince("").setKeyTo("z").build())
                        .forEachRemaining(e -> {}));
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    }

    @Test
    @Order(13)
    void range_keySinceAfterKeyTo_returnsInvalidArgument() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
                () -> stub.range(RangeRequest.newBuilder().setKeySince("z").setKeyTo("a").build())
                        .forEachRemaining(e -> {}));
        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    }

    // ── count ────────────────────────────────────────────────────────────────

    @Test
    @Order(8)
    void count_reflectsNumberOfRecords() {
        long before = stub.count(Empty.getDefaultInstance()).getCount();

        stub.put(PutRequest.newBuilder()
                .setKey("count-key-" + System.nanoTime())
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("v")))
                .build());

        long after = stub.count(Empty.getDefaultInstance()).getCount();
        assertEquals(before + 1, after);
    }
}
