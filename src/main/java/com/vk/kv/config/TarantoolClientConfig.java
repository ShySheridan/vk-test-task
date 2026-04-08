package com.vk.kv.config;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;

import java.util.Objects;

public final class TarantoolClientConfig {

    private TarantoolClientConfig() {
    }

    public static TarantoolBoxClient buildClient(AppConfig appConfig) {
        Objects.requireNonNull(appConfig, "appConfig must not be null");

        try {
            return TarantoolFactory.box()
                    .withHost(appConfig.tarantoolHost())
                    .withPort(appConfig.tarantoolPort())
                    .withUser(appConfig.tarantoolUser())
                    .withPassword(appConfig.tarantoolPassword())
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create Tarantool client for "
                            + appConfig.tarantoolHost()
                            + ":"
                            + appConfig.tarantoolPort(),
                    e
            );
        }
    }
}