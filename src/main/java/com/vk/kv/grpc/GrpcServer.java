package com.vk.kv.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class GrpcServer {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5L;
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65_535;

    private final Server server;

    public GrpcServer(int port, BindableService service) {
        validatePort(port);
        Objects.requireNonNull(service, "service must not be null");

        this.server = ServerBuilder.forPort(port)
                .addService(service)
                .build();
    }

    public void start() throws IOException {
        server.start();
        log.info("gRPC server started");
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public void stop() {
        log.info("Stopping gRPC server");
        server.shutdown();

        try {
            if (!server.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn(
                        "gRPC server did not terminate within {} seconds, forcing shutdown",
                        SHUTDOWN_TIMEOUT_SECONDS
                );
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while stopping gRPC server, forcing shutdown", e);
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void validatePort(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException(
                    "port must be in range " + MIN_PORT + ".." + MAX_PORT + ": " + port
            );
        }
    }
}