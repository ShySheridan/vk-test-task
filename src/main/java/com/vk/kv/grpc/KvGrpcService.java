package com.vk.kv.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.vk.kv.model.KvRecord;
import com.vk.kv.proto.*;
import com.vk.kv.service.KvService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Optional;

/**
 * Transport layer: validates incoming proto requests, converts between
 * proto ↔ domain objects, and delegates to {@link KvService}.
 */
public class KvGrpcService extends KvServiceGrpc.KvServiceImplBase {

    private final KvService kvService;

    public KvGrpcService(KvService kvService) {
        this.kvService = kvService;
    }


    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> obs) {
        if (rejectIfBlank(request.getKey(), obs)) return;
        try {
            kvService.put(request.getKey(), toBytes(request.getValue()));
            obs.onNext(PutResponse.getDefaultInstance());
            obs.onCompleted();
        } catch (Exception e) {
            obs.onError(internal(e));
        }
    }


    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> obs) {
        if (rejectIfBlank(request.getKey(), obs)) return;
        try {
            Optional<KvRecord> record = kvService.get(request.getKey());
            if (record.isEmpty()) {
                obs.onError(Status.NOT_FOUND
                        .withDescription("key not found: " + request.getKey())
                        .asRuntimeException());
                return;
            }
            obs.onNext(GetResponse.newBuilder()
                    .setKey(record.get().key())
                    .setValue(toNullableBytes(record.get().value()))
                    .build());
            obs.onCompleted();
        } catch (Exception e) {
            obs.onError(internal(e));
        }
    }


    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> obs) {
        if (rejectIfBlank(request.getKey(), obs)) return;
        try {
            boolean deleted = kvService.delete(request.getKey());
            obs.onNext(DeleteResponse.newBuilder().setDeleted(deleted).build());
            obs.onCompleted();
        } catch (Exception e) {
            obs.onError(internal(e));
        }
    }


    // Semantics: [key_since, key_to] — both endpoints inclusive.
    @Override
    public void range(RangeRequest request, StreamObserver<RangeEntry> obs) {
        if (rejectIfBlank(request.getKeySince(), "key_since", obs)) return;
        if (rejectIfBlank(request.getKeyTo(),    "key_to",    obs)) return;
        if (request.getKeySince().compareTo(request.getKeyTo()) > 0) {
            obs.onError(Status.INVALID_ARGUMENT
                    .withDescription("key_since must be <= key_to").asRuntimeException());
            return;
        }
        try {
            kvService.range(request.getKeySince(), request.getKeyTo(),
                    record -> obs.onNext(RangeEntry.newBuilder()
                            .setKey(record.key())
                            .setValue(toNullableBytes(record.value()))
                            .build()));
            obs.onCompleted();
        } catch (Exception e) {
            obs.onError(internal(e));
        }
    }


    @Override
    public void count(Empty request, StreamObserver<CountResponse> obs) {
        try {
            obs.onNext(CountResponse.newBuilder().setCount(kvService.count()).build());
            obs.onCompleted();
        } catch (Exception e) {
            obs.onError(internal(e));
        }
    }


    private byte[] toBytes(NullableBytes nb) {
        if (nb == null || nb.getKindCase() != NullableBytes.KindCase.DATA) return null;
        return nb.getData().toByteArray();
    }

    private NullableBytes toNullableBytes(byte[] value) {
        if (value == null) {
            return NullableBytes.newBuilder().setNullValue(Empty.getDefaultInstance()).build();
        }
        return NullableBytes.newBuilder().setData(ByteString.copyFrom(value)).build();
    }


    private boolean rejectIfBlank(String key, StreamObserver<?> obs) {
        return rejectIfBlank(key, "key", obs);
    }

    private boolean rejectIfBlank(String value, String fieldName, StreamObserver<?> obs) {
        if (value == null || value.isBlank()) {
            obs.onError(Status.INVALID_ARGUMENT
                    .withDescription(fieldName + " must not be empty").asRuntimeException());
            return true;
        }
        return false;
    }

    private RuntimeException internal(Exception e) {
        return Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException();
    }
}
