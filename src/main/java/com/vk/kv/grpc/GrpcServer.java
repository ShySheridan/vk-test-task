package com.vk.kv.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class GrpcServer {

    private final Server server;

    public GrpcServer(int port, KvGrpcService service) {
        this.server = ServerBuilder.forPort(port)
                .addService(service)
                .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    /** Graceful shutdown: waits up to 5 s for in-flight calls, then forces stop. */
    public void stop() {
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
