package com.vk.kv;

import com.vk.kv.config.AppConfig;
import com.vk.kv.config.TarantoolClientConfig;
import com.vk.kv.grpc.GrpcServer;
import com.vk.kv.grpc.KvGrpcService;
import com.vk.kv.repository.TarantoolKvRepository;
import com.vk.kv.service.KvServiceImpl;
import io.tarantool.client.box.TarantoolBoxClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class VkTestTaskApplication {

    private static final int GRPC_PORT =
            Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "9090"));

    private VkTestTaskApplication() {
    }

    public static void main(String[] args) throws Exception {
        AppConfig appConfig = AppConfig.fromEnv();
        TarantoolBoxClient tarantoolClient = TarantoolClientConfig.buildClient(appConfig);

        TarantoolKvRepository repository = new TarantoolKvRepository(tarantoolClient);
        KvServiceImpl service = new KvServiceImpl(repository);
        KvGrpcService grpcService = new KvGrpcService(service);
        GrpcServer grpcServer = new GrpcServer(GRPC_PORT, grpcService);

        grpcServer.start();
        log.info("gRPC KV service started on port {}", GRPC_PORT);

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> shutdown(grpcServer, tarantoolClient), "shutdown-hook")
        );

        grpcServer.awaitTermination();
    }

    private static void shutdown(GrpcServer grpcServer, TarantoolBoxClient tarantoolClient) {
        log.info("Application shutdown started");

        try {
            grpcServer.stop();
            log.info("gRPC server stopped");
        } catch (RuntimeException e) {
            log.error("Failed to stop gRPC server", e);
        }

        try {
            tarantoolClient.close();
            log.info("Tarantool client closed");
        } catch (Exception e) {
            log.error("Failed to close Tarantool client", e);
        }
    }
}