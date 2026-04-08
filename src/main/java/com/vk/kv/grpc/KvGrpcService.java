package com.vk.kv.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.vk.kv.model.KvRecord;
import com.vk.kv.proto.CountResponse;
import com.vk.kv.proto.DeleteRequest;
import com.vk.kv.proto.DeleteResponse;
import com.vk.kv.proto.GetRequest;
import com.vk.kv.proto.GetResponse;
import com.vk.kv.proto.KvServiceGrpc;
import com.vk.kv.proto.NullableBytes;
import com.vk.kv.proto.PutRequest;
import com.vk.kv.proto.PutResponse;
import com.vk.kv.proto.RangeEntry;
import com.vk.kv.proto.RangeRequest;
import com.vk.kv.service.KvService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;

@Slf4j
public final class KvGrpcService extends KvServiceGrpc.KvServiceImplBase {

    private static final String INTERNAL_ERROR_MESSAGE = "Internal server error";

    private final KvService kvService;

    public KvGrpcService(KvService kvService) {
        this.kvService = Objects.requireNonNull(kvService, "kvService must not be null");
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        if (rejectIfBlank(request.getKey(), responseObserver)) {
            return;
        }

        try {
            kvService.put(request.getKey(), toBytes(request.getValue()));
            responseObserver.onNext(PutResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            log.error("put failed for key={}", request.getKey(), e);
            responseObserver.onError(internal(e));
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        if (rejectIfBlank(request.getKey(), responseObserver)) {
            return;
        }

        try {
            Optional<KvRecord> optionalRecord = kvService.get(request.getKey());
            if (optionalRecord.isEmpty()) {
                responseObserver.onError(
                        Status.NOT_FOUND
                                .withDescription("key not found: " + request.getKey())
                                .asRuntimeException()
                );
                return;
            }

            KvRecord record = optionalRecord.get();
            responseObserver.onNext(
                    GetResponse.newBuilder()
                            .setKey(record.key())
                            .setValue(toNullableBytes(record.value()))
                            .build()
            );
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            log.error("get failed for key={}", request.getKey(), e);
            responseObserver.onError(internal(e));
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        if (rejectIfBlank(request.getKey(), responseObserver)) {
            return;
        }

        try {
            boolean deleted = kvService.delete(request.getKey());
            responseObserver.onNext(DeleteResponse.newBuilder().setDeleted(deleted).build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            log.error("delete failed for key={}", request.getKey(), e);
            responseObserver.onError(internal(e));
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeEntry> responseObserver) {
        if (rejectIfBlank(request.getKeySince(), "key_since", responseObserver)) {
            return;
        }
        if (rejectIfBlank(request.getKeyTo(), "key_to", responseObserver)) {
            return;
        }
        if (request.getKeySince().compareTo(request.getKeyTo()) > 0) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription("key_since must be <= key_to")
                            .asRuntimeException()
            );
            return;
        }

        try {
            kvService.range(
                    request.getKeySince(),
                    request.getKeyTo(),
                    record -> responseObserver.onNext(
                            RangeEntry.newBuilder()
                                    .setKey(record.key())
                                    .setValue(toNullableBytes(record.value()))
                                    .build()
                    )
            );
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            log.error(
                    "range failed for keySince={} keyTo={}",
                    request.getKeySince(),
                    request.getKeyTo(),
                    e
            );
            responseObserver.onError(internal(e));
        }
    }

    @Override
    public void count(Empty request, StreamObserver<CountResponse> responseObserver) {
        try {
            responseObserver.onNext(
                    CountResponse.newBuilder()
                            .setCount(kvService.count())
                            .build()
            );
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            log.error("count failed", e);
            responseObserver.onError(internal(e));
        }
    }

    private static byte[] toBytes(NullableBytes nullableBytes) {
        if (nullableBytes == null || nullableBytes.getKindCase() != NullableBytes.KindCase.DATA) {
            return null;
        }
        return nullableBytes.getData().toByteArray();
    }

    private static NullableBytes toNullableBytes(byte[] value) {
        if (value == null) {
            return NullableBytes.newBuilder()
                    .setNullValue(Empty.getDefaultInstance())
                    .build();
        }
        return NullableBytes.newBuilder()
                .setData(ByteString.copyFrom(value))
                .build();
    }

    private boolean rejectIfBlank(String key, StreamObserver<?> responseObserver) {
        return rejectIfBlank(key, "key", responseObserver);
    }

    private boolean rejectIfBlank(
            String value,
            String fieldName,
            StreamObserver<?> responseObserver
    ) {
        if (value == null || value.isBlank()) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(fieldName + " must not be empty")
                            .asRuntimeException()
            );
            return true;
        }
        return false;
    }

    private RuntimeException internal(RuntimeException exception) {
        return Status.INTERNAL
                .withDescription(INTERNAL_ERROR_MESSAGE)
                .withCause(exception)
                .asRuntimeException();
    }
}