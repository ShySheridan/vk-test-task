package com.vk.kv.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.vk.kv.model.KvRecord;
import com.vk.kv.proto.CountResponse;
import com.vk.kv.proto.DeleteRequest;
import com.vk.kv.proto.DeleteResponse;
import com.vk.kv.proto.GetRequest;
import com.vk.kv.proto.GetResponse;
import com.vk.kv.proto.NullableBytes;
import com.vk.kv.proto.PutRequest;
import com.vk.kv.proto.PutResponse;
import com.vk.kv.proto.RangeEntry;
import com.vk.kv.proto.RangeRequest;
import com.vk.kv.service.KvService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KvGrpcServiceTest {

    private static final String INTERNAL_SERVER_ERROR = "Internal server error";

    @Mock
    private KvService kvService;

    @Mock
    private StreamObserver<PutResponse> putObserver;

    @Mock
    private StreamObserver<GetResponse> getObserver;

    @Mock
    private StreamObserver<DeleteResponse> deleteObserver;

    @Mock
    private StreamObserver<RangeEntry> rangeObserver;

    @Mock
    private StreamObserver<CountResponse> countObserver;

    private KvGrpcService grpcService;

    @BeforeEach
    void setUp() {
        grpcService = new KvGrpcService(kvService);
    }

    @Test
    void put_whenValueContainsBytes_callsServiceAndCompletes() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);

        grpcService.put(
                PutRequest.newBuilder()
                        .setKey("key-1")
                        .setValue(bytesValue(payload))
                        .build(),
                putObserver
        );

        verify(kvService).put(eq("key-1"), eq(payload));

        InOrder inOrder = inOrder(putObserver);
        inOrder.verify(putObserver).onNext(PutResponse.getDefaultInstance());
        inOrder.verify(putObserver).onCompleted();

        verify(putObserver, never()).onError(any());
        verifyNoMoreInteractions(putObserver);
        verifyNoMoreInteractions(kvService);
    }

    @Test
    void put_whenValueIsNull_callsServiceWithNullAndCompletes() {
        grpcService.put(
                PutRequest.newBuilder()
                        .setKey("key-null")
                        .setValue(nullValue())
                        .build(),
                putObserver
        );

        verify(kvService).put("key-null", null);

        InOrder inOrder = inOrder(putObserver);
        inOrder.verify(putObserver).onNext(PutResponse.getDefaultInstance());
        inOrder.verify(putObserver).onCompleted();

        verify(putObserver, never()).onError(any());
        verifyNoMoreInteractions(putObserver);
        verifyNoMoreInteractions(kvService);
    }

    @Test
    void put_whenKeyIsBlank_returnsInvalidArgumentAndDoesNotCallService() {
        grpcService.put(
                PutRequest.newBuilder()
                        .setKey("   ")
                        .setValue(bytesValue("v".getBytes(StandardCharsets.UTF_8)))
                        .build(),
                putObserver
        );

        verifyNoInteractions(kvService);
        assertGrpcError(captureError(putObserver), Status.Code.INVALID_ARGUMENT, "key must not be empty");
        verify(putObserver, never()).onNext(any());
        verify(putObserver, never()).onCompleted();
        verifyNoMoreInteractions(putObserver);
    }

    @Test
    void put_whenServiceFails_returnsInternalError() {
        doThrow(new IllegalStateException("storage down"))
                .when(kvService).put(eq("key-1"), any());

        grpcService.put(
                PutRequest.newBuilder()
                        .setKey("key-1")
                        .setValue(bytesValue("v".getBytes(StandardCharsets.UTF_8)))
                        .build(),
                putObserver
        );

        verify(kvService).put(eq("key-1"), any());
        assertInternalError(captureError(putObserver));
        verify(putObserver, never()).onNext(any());
        verify(putObserver, never()).onCompleted();
        verifyNoMoreInteractions(putObserver);
        verifyNoMoreInteractions(kvService);
    }

    @Test
    void get_whenRecordExistsWithBytes_returnsResponseAndCompletes() {
        byte[] payload = "value".getBytes(StandardCharsets.UTF_8);
        when(kvService.get("key-2")).thenReturn(Optional.of(new KvRecord("key-2", payload)));

        grpcService.get(GetRequest.newBuilder().setKey("key-2").build(), getObserver);

        verify(kvService).get("key-2");

        ArgumentCaptor<GetResponse> responseCaptor = ArgumentCaptor.forClass(GetResponse.class);
        InOrder inOrder = inOrder(getObserver);
        inOrder.verify(getObserver).onNext(responseCaptor.capture());
        inOrder.verify(getObserver).onCompleted();

        verify(getObserver, never()).onError(any());
        verifyNoMoreInteractions(getObserver);
        verifyNoMoreInteractions(kvService);

        GetResponse response = responseCaptor.getValue();
        assertEquals("key-2", response.getKey());
        assertEquals(NullableBytes.KindCase.DATA, response.getValue().getKindCase());
        assertArrayEquals(payload, response.getValue().getData().toByteArray());
    }

    @Test
    void get_whenRecordExistsWithNull_returnsNullValueAndCompletes() {
        when(kvService.get("key-null")).thenReturn(Optional.of(new KvRecord("key-null", null)));

        grpcService.get(GetRequest.newBuilder().setKey("key-null").build(), getObserver);

        verify(kvService).get("key-null");

        ArgumentCaptor<GetResponse> responseCaptor = ArgumentCaptor.forClass(GetResponse.class);
        InOrder inOrder = inOrder(getObserver);
        inOrder.verify(getObserver).onNext(responseCaptor.capture());
        inOrder.verify(getObserver).onCompleted();

        verify(getObserver, never()).onError(any());
        verifyNoMoreInteractions(getObserver);
        verifyNoMoreInteractions(kvService);

        GetResponse response = responseCaptor.getValue();
        assertEquals("key-null", response.getKey());
        assertEquals(NullableBytes.KindCase.NULL_VALUE, response.getValue().getKindCase());
    }

    @Test
    void get_whenKeyIsMissing_returnsNotFound() {
        when(kvService.get("missing")).thenReturn(Optional.empty());

        grpcService.get(GetRequest.newBuilder().setKey("missing").build(), getObserver);

        verify(kvService).get("missing");
        assertGrpcError(captureError(getObserver), Status.Code.NOT_FOUND, "key not found: missing");
        verify(getObserver, never()).onNext(any());
        verify(getObserver, never()).onCompleted();
        verifyNoMoreInteractions(getObserver);
        verifyNoMoreInteractions(kvService);
    }

    @Test
    void get_whenKeyIsBlank_returnsInvalidArgumentAndDoesNotCallService() {
        grpcService.get(GetRequest.newBuilder().setKey(" ").build(), getObserver);

        verifyNoInteractions(kvService);
        assertGrpcError(captureError(getObserver), Status.Code.INVALID_ARGUMENT, "key must not be empty");
        verify(getObserver, never()).onNext(any());
        verify(getObserver, never()).onCompleted();
        verifyNoMoreInteractions(getObserver);
    }

    @Test
    void get_whenServiceFails_returnsInternalError() {
        when(kvService.get("key-1")).thenThrow(new IllegalStateException("storage down"));

        grpcService.get(GetRequest.newBuilder().setKey("key-1").build(), getObserver);

        verify(kvService).get("key-1");
        assertInternalError(captureError(getObserver));
        verify(getObserver, never()).onNext(any());
        verify(getObserver, never()).onCompleted();
        verifyNoMoreInteractions(getObserver);
        verifyNoMoreInteractions(kvService);
    }

    @Test
    void delete_whenKeyExists_returnsDeletedTrueAndCompletes() {
        when(kvService.delete("key-del")).thenReturn(true);

        grpcService.delete(DeleteRequest.newBuilder().setKey("key-del").build(), deleteObserver);

        verify(kvService).delete("key-del");

        ArgumentCaptor<DeleteResponse> responseCaptor = ArgumentCaptor.forClass(DeleteResponse.class);
        InOrder inOrder = inOrder(deleteObserver);
        inOrder.verify(deleteObserver).onNext(responseCaptor.capture());
        inOrder.verify(deleteObserver).onCompleted();

        verify(deleteObserver, never()).onError(any());
        verifyNoMoreInteractions(deleteObserver);
        verifyNoMoreInteractions(kvService);

        assertTrue(responseCaptor.getValue().getDeleted());
    }

    @Test
    void delete_whenKeyDoesNotExist_returnsDeletedFalseAndCompletes() {
        when(kvService.delete("missing")).thenReturn(false);

        grpcService.delete(DeleteRequest.newBuilder().setKey("missing").build(), deleteObserver);

        verify(kvService).delete("missing");

        ArgumentCaptor<DeleteResponse> responseCaptor = ArgumentCaptor.forClass(DeleteResponse.class);
        InOrder inOrder = inOrder(deleteObserver);
        inOrder.verify(deleteObserver).onNext(responseCaptor.capture());
        inOrder.verify(deleteObserver).onCompleted();

        verify(deleteObserver, never()).onError(any());
        verifyNoMoreInteractions(deleteObserver);
        verifyNoMoreInteractions(kvService);

        assertEquals(false, responseCaptor.getValue().getDeleted());
    }

    @Test
    void delete_whenKeyIsBlank_returnsInvalidArgumentAndDoesNotCallService() {
        grpcService.delete(DeleteRequest.newBuilder().setKey(" ").build(), deleteObserver);

        verifyNoInteractions(kvService);
        assertGrpcError(captureError(deleteObserver), Status.Code.INVALID_ARGUMENT, "key must not be empty");
        verify(deleteObserver, never()).onNext(any());
        verify(deleteObserver, never()).onCompleted();
        verifyNoMoreInteractions(deleteObserver);
    }

    @Test
    void delete_whenServiceFails_returnsInternalError() {
        when(kvService.delete("key-del")).thenThrow(new IllegalStateException("storage down"));

        grpcService.delete(DeleteRequest.newBuilder().setKey("key-del").build(), deleteObserver);

        verify(kvService).delete("key-del");
        assertInternalError(captureError(deleteObserver));
        verify(deleteObserver, never()).onNext(any());
        verify(deleteObserver, never()).onCompleted();
        verifyNoMoreInteractions(deleteObserver);
        verifyNoMoreInteractions(kvService);
    }

    @Test
    void range_whenRequestIsValid_streamsAllEntriesAndCompletes() {
        doAnswer(invocation -> {
            Consumer<KvRecord> sink = invocation.getArgument(2);
            sink.accept(new KvRecord("a", "1".getBytes(StandardCharsets.UTF_8)));
            sink.accept(new KvRecord("b", null));
            return null;
        }).when(kvService).range(eq("a"), eq("b"), any());

        grpcService.range(
                RangeRequest.newBuilder()
                        .setKeySince("a")
                        .setKeyTo("b")
                        .build(),
                rangeObserver
        );

        verify(kvService).range(eq("a"), eq("b"), any());

        ArgumentCaptor<RangeEntry> entryCaptor = ArgumentCaptor.forClass(RangeEntry.class);
        verify(rangeObserver, never()).onError(any());
        verify(rangeObserver).onCompleted();
        verify(rangeObserver, org.mockito.Mockito.times(2)).onNext(entryCaptor.capture());
        verifyNoMoreInteractions(rangeObserver);
        verifyNoMoreInteractions(kvService);

        List<RangeEntry> entries = entryCaptor.getAllValues();
        assertEquals(2, entries.size());

        RangeEntry first = entries.get(0);
        assertEquals("a", first.getKey());
        assertEquals(NullableBytes.KindCase.DATA, first.getValue().getKindCase());
        assertArrayEquals("1".getBytes(StandardCharsets.UTF_8), first.getValue().getData().toByteArray());

        RangeEntry second = entries.get(1);
        assertEquals("b", second.getKey());
        assertEquals(NullableBytes.KindCase.NULL_VALUE, second.getValue().getKindCase());
    }

    @Test
    void range_whenKeySinceIsBlank_returnsInvalidArgumentAndDoesNotCallService() {
        grpcService.range(
                RangeRequest.newBuilder()
                        .setKeySince(" ")
                        .setKeyTo("z")
                        .build(),
                rangeObserver
        );

        verifyNoInteractions(kvService);
        assertGrpcError(captureError(rangeObserver), Status.Code.INVALID_ARGUMENT, "key_since must not be empty");
        verify(rangeObserver, never()).onNext(any());
        verify(rangeObserver, never()).onCompleted();
        verifyNoMoreInteractions(rangeObserver);
    }

    @Test
    void range_whenKeyToIsBlank_returnsInvalidArgumentAndDoesNotCallService() {
        grpcService.range(
                RangeRequest.newBuilder()
                        .setKeySince("a")
                        .setKeyTo(" ")
                        .build(),
                rangeObserver
        );

        verifyNoInteractions(kvService);
        assertGrpcError(captureError(rangeObserver), Status.Code.INVALID_ARGUMENT, "key_to must not be empty");
        verify(rangeObserver, never()).onNext(any());
        verify(rangeObserver, never()).onCompleted();
        verifyNoMoreInteractions(rangeObserver);
    }

    @Test
    void range_whenKeySinceIsAfterKeyTo_returnsInvalidArgumentAndDoesNotCallService() {
        grpcService.range(
                RangeRequest.newBuilder()
                        .setKeySince("z")
                        .setKeyTo("a")
                        .build(),
                rangeObserver
        );

        verifyNoInteractions(kvService);
        assertGrpcError(captureError(rangeObserver), Status.Code.INVALID_ARGUMENT, "key_since must be <= key_to");
        verify(rangeObserver, never()).onNext(any());
        verify(rangeObserver, never()).onCompleted();
        verifyNoMoreInteractions(rangeObserver);
    }

    @Test
    void range_whenServiceFails_returnsInternalError() {
        doThrow(new IllegalStateException("range failed"))
                .when(kvService).range(eq("a"), eq("b"), any());

        grpcService.range(
                RangeRequest.newBuilder()
                        .setKeySince("a")
                        .setKeyTo("b")
                        .build(),
                rangeObserver
        );

        verify(kvService).range(eq("a"), eq("b"), any());
        assertInternalError(captureError(rangeObserver));
        verify(rangeObserver, never()).onNext(any());
        verify(rangeObserver, never()).onCompleted();
        verifyNoMoreInteractions(rangeObserver);
        verifyNoMoreInteractions(kvService);
    }

    @Test
    void count_whenServiceReturnsValue_returnsResponseAndCompletes() {
        when(kvService.count()).thenReturn(42L);

        grpcService.count(Empty.getDefaultInstance(), countObserver);

        verify(kvService).count();

        ArgumentCaptor<CountResponse> responseCaptor = ArgumentCaptor.forClass(CountResponse.class);
        InOrder inOrder = inOrder(countObserver);
        inOrder.verify(countObserver).onNext(responseCaptor.capture());
        inOrder.verify(countObserver).onCompleted();

        verify(countObserver, never()).onError(any());
        verifyNoMoreInteractions(countObserver);
        verifyNoMoreInteractions(kvService);

        assertEquals(42L, responseCaptor.getValue().getCount());
    }

    @Test
    void count_whenServiceFails_returnsInternalError() {
        when(kvService.count()).thenThrow(new IllegalStateException("count failed"));

        grpcService.count(Empty.getDefaultInstance(), countObserver);

        verify(kvService).count();
        assertInternalError(captureError(countObserver));
        verify(countObserver, never()).onNext(any());
        verify(countObserver, never()).onCompleted();
        verifyNoMoreInteractions(countObserver);
        verifyNoMoreInteractions(kvService);
    }

    private static NullableBytes bytesValue(byte[] payload) {
        return NullableBytes.newBuilder()
                .setData(ByteString.copyFrom(payload))
                .build();
    }

    private static NullableBytes nullValue() {
        return NullableBytes.newBuilder()
                .setNullValue(Empty.getDefaultInstance())
                .build();
    }

    private static Throwable captureError(StreamObserver<?> observer) {
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(captor.capture());
        return captor.getValue();
    }

    private static void assertInternalError(Throwable throwable) {
        assertGrpcError(throwable, Status.Code.INTERNAL, INTERNAL_SERVER_ERROR);
    }

    private static void assertGrpcError(
            Throwable throwable,
            Status.Code expectedCode,
            String expectedDescription
    ) {
        assertInstanceOf(StatusRuntimeException.class, throwable);

        StatusRuntimeException exception = (StatusRuntimeException) throwable;
        assertEquals(expectedCode, exception.getStatus().getCode());
        assertEquals(expectedDescription, exception.getStatus().getDescription());
    }
}