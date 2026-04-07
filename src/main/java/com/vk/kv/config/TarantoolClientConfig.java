package com.vk.kv.config;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;

public class TarantoolClientConfig {

    private static final String HOST =
            System.getenv().getOrDefault("TARANTOOL_HOST", "localhost");
    private static final int PORT =
            Integer.parseInt(System.getenv().getOrDefault("TARANTOOL_PORT", "3301"));
    private static final String USER =
            System.getenv().getOrDefault("TARANTOOL_USER", "app");
    private static final String PASSWORD =
            System.getenv().getOrDefault("TARANTOOL_PASSWORD", "app_password");

    public static TarantoolBoxClient buildClient() throws Exception {
        return TarantoolFactory.box()
                .withHost(HOST)
                .withPort(PORT)
                .withUser(USER)
                .withPassword(PASSWORD)
                .build();
    }
}
