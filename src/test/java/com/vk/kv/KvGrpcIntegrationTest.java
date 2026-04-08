package com.vk.kv;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.vk.kv.config.AppConfig;
import com.vk.kv.config.TarantoolClientConfig;
import com.vk.kv.grpc.KvGrpcService;
import com.vk.kv.proto.DeleteRequest;
import com.vk.kv.proto.DeleteResponse;
import com.vk.kv.proto.GetRequest;
import com.vk.kv.proto.GetResponse;
import com.vk.kv.proto.KvServiceGrpc;
import com.vk.kv.proto.NullableBytes;
import com.vk.kv.proto.PutRequest;
import com.vk.kv.proto.RangeEntry;
import com.vk.kv.proto.RangeRequest;
import com.vk.kv.repository.TarantoolKvRepository;
import com.vk.kv.service.KvServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KvGrpcIntegrationTest {

    private TarantoolBoxClient tarantoolClient;
    private Server grpcServer;
    private ManagedChannel channel;
    private KvServiceGrpc.KvServiceBlockingStub stub;

    private final String runId = UUID.randomUUID().toString();

    @BeforeAll
    void setUp() throws Exception {
        AppConfig appConfig = AppConfig.fromEnv();
        tarantoolClient = TarantoolClientConfig.buildClient(appConfig);

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
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (grpcServer != null) {
            grpcServer.shutdownNow();
        }
        if (tarantoolClient != null) {
            tarantoolClient.close();
        }
    }

    @Test
    void putAndGet_regularValue() {
        String key = key("put-get");
        byte[] payload = "hello".getBytes();

        putBytes(key, payload);

        GetResponse response = stub.get(GetRequest.newBuilder().setKey(key).build());

        assertEquals(key, response.getKey());
        assertEquals(NullableBytes.KindCase.DATA, response.getValue().getKindCase());
        assertArrayEquals(payload, response.getValue().getData().toByteArray());
    }

    @Test
    void putAndGet_nullValue() {
        String key = key("null-value");

        putNull(key);

        GetResponse response = stub.get(GetRequest.newBuilder().setKey(key).build());

        assertEquals(key, response.getKey());
        assertEquals(NullableBytes.KindCase.NULL_VALUE, response.getValue().getKindCase());
    }

    @Test
    void put_overwritesExistingKey() {
        String key = key("overwrite");

        putBytes(key, "v1".getBytes());
        putBytes(key, "v2".getBytes());

        GetResponse response = stub.get(GetRequest.newBuilder().setKey(key).build());

        assertArrayEquals("v2".getBytes(), response.getValue().getData().toByteArray());
    }

    @Test
    void get_missingKey_returnsNotFound() {
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.get(GetRequest.newBuilder().setKey(key("missing")).build())
        );

        assertEquals(Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void delete_existingKey_returnsTrue_andKeyBecomesUnavailable() {
        String key = key("delete");

        putBytes(key, "x".getBytes());

        DeleteResponse deleteResponse =
                stub.delete(DeleteRequest.newBuilder().setKey(key).build());

        assertTrue(deleteResponse.getDeleted());

        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.get(GetRequest.newBuilder().setKey(key).build())
        );

        assertEquals(Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void delete_missingKey_returnsFalse() {
        DeleteResponse response =
                stub.delete(DeleteRequest.newBuilder().setKey(key("missing-delete")).build());

        assertFalse(response.getDeleted());
    }

    @Test
    void range_returnsExactSortedSlice_includingBothBounds() {
        String a = key("range-a");
        String b = key("range-b");
        String c = key("range-c");
        String d = key("range-d");

        putBytes(a, a.getBytes());
        putBytes(b, b.getBytes());
        putBytes(c, c.getBytes());
        putBytes(d, d.getBytes());

        Iterator<RangeEntry> iterator = stub.range(
                RangeRequest.newBuilder()
                        .setKeySince(a)
                        .setKeyTo(c)
                        .build()
        );

        List<String> actualKeys = new ArrayList<>();
        iterator.forEachRemaining(entry -> actualKeys.add(entry.getKey()));

        assertEquals(List.of(a, b, c), actualKeys);
    }

    @Test
    void range_equalBounds_returnsSingleRecord() {
        String key = key("range-single");

        putBytes(key, "value".getBytes());

        Iterator<RangeEntry> iterator = stub.range(
                RangeRequest.newBuilder()
                        .setKeySince(key)
                        .setKeyTo(key)
                        .build()
        );

        List<String> actualKeys = new ArrayList<>();
        iterator.forEachRemaining(entry -> actualKeys.add(entry.getKey()));

        assertEquals(List.of(key), actualKeys);
    }

    @Test
    void put_blankKey_returnsInvalidArgument() {
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.put(PutRequest.newBuilder()
                        .setKey("   ")
                        .setValue(NullableBytes.newBuilder().setData(ByteString.copyFromUtf8("v")))
                        .build())
        );

        assertEquals(Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    }

    @Test
    void range_blankKeyTo_returnsInvalidArgument() {
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.range(RangeRequest.newBuilder()
                                .setKeySince(key("left"))
                                .setKeyTo(" ")
                                .build())
                        .forEachRemaining(entry -> {})
        );

        assertEquals(Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    }

    private String key(String suffix) {
        return "it-" + runId + "-" + suffix;
    }

    private void putBytes(String key, byte[] value) {
        stub.put(PutRequest.newBuilder()
                .setKey(key)
                .setValue(NullableBytes.newBuilder().setData(ByteString.copyFrom(value)))
                .build());
    }

    private void putNull(String key) {
        stub.put(PutRequest.newBuilder()
                .setKey(key)
                .setValue(NullableBytes.newBuilder().setNullValue(Empty.getDefaultInstance()))
                .build());
    }
}