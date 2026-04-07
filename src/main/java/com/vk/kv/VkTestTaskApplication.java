package com.vk.kv;

import com.vk.kv.config.TarantoolClientConfig;
import com.vk.kv.grpc.GrpcServer;
import com.vk.kv.grpc.KvGrpcService;
import com.vk.kv.repository.TarantoolKvRepository;
import com.vk.kv.service.KvServiceImpl;
import io.tarantool.client.box.TarantoolBoxClient;

public class VkTestTaskApplication {

    private static final int GRPC_PORT =
            Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "9090"));

    public static void main(String[] args) throws Exception {
        TarantoolBoxClient     tarantoolClient = TarantoolClientConfig.buildClient();
        TarantoolKvRepository  repository      = new TarantoolKvRepository(tarantoolClient);
        KvServiceImpl          service         = new KvServiceImpl(repository);
        KvGrpcService          grpcService     = new KvGrpcService(service);
        GrpcServer             grpcServer      = new GrpcServer(GRPC_PORT, grpcService);

        grpcServer.start();
        System.out.println("gRPC KV service started on port " + GRPC_PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            grpcServer.stop();
            try { tarantoolClient.close(); } catch (Exception ignored) {}
        }));

        grpcServer.awaitTermination();
    }
}
