package com.vk.kv.config;

import java.util.Map;
import java.util.Objects;

public final class AppConfig {

    private static final String TARANTOOL_HOST_ENV = "TARANTOOL_HOST";
    private static final String TARANTOOL_PORT_ENV = "TARANTOOL_PORT";
    private static final String TARANTOOL_USER_ENV = "TARANTOOL_USER";
    private static final String TARANTOOL_PASSWORD_ENV = "TARANTOOL_PASSWORD";

    private static final String DEFAULT_TARANTOOL_HOST = "localhost";
    private static final int DEFAULT_TARANTOOL_PORT = 3301;
    private static final String DEFAULT_TARANTOOL_USER = "app";
    private static final String DEFAULT_TARANTOOL_PASSWORD = "app_password";

    private final String tarantoolHost;
    private final int tarantoolPort;
    private final String tarantoolUser;
    private final String tarantoolPassword;

    private AppConfig(
            String tarantoolHost,
            int tarantoolPort,
            String tarantoolUser,
            String tarantoolPassword
    ) {
        this.tarantoolHost = requireNonBlank(tarantoolHost, TARANTOOL_HOST_ENV);
        this.tarantoolPort = validatePort(tarantoolPort, TARANTOOL_PORT_ENV);
        this.tarantoolUser = requireNonBlank(tarantoolUser, TARANTOOL_USER_ENV);
        this.tarantoolPassword = Objects.requireNonNull(tarantoolPassword, TARANTOOL_PASSWORD_ENV + " must not be null");
    }

    public static AppConfig fromEnv() {
        Map<String, String> env = System.getenv();

        String host = env.getOrDefault(TARANTOOL_HOST_ENV, DEFAULT_TARANTOOL_HOST);
        String portRaw = env.getOrDefault(TARANTOOL_PORT_ENV, String.valueOf(DEFAULT_TARANTOOL_PORT));
        String user = env.getOrDefault(TARANTOOL_USER_ENV, DEFAULT_TARANTOOL_USER);
        String password = env.getOrDefault(TARANTOOL_PASSWORD_ENV, DEFAULT_TARANTOOL_PASSWORD);

        return new AppConfig(
                host,
                parsePort(portRaw, TARANTOOL_PORT_ENV),
                user,
                password
        );
    }

    public String tarantoolHost() {
        return tarantoolHost;
    }

    public int tarantoolPort() {
        return tarantoolPort;
    }

    public String tarantoolUser() {
        return tarantoolUser;
    }

    public String tarantoolPassword() {
        return tarantoolPassword;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(fieldName + " must not be blank");
        }
        return value;
    }

    private static int parsePort(String rawValue, String fieldName) {
        try {
            return Integer.parseInt(requireNonBlank(rawValue, fieldName));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(fieldName + " must be a valid integer: " + rawValue, e);
        }
    }

    private static int validatePort(int port, String fieldName) {
        if (port < 1 || port > 65535) {
            throw new IllegalStateException(fieldName + " must be in range 1..65535: " + port);
        }
        return port;
    }

    public static AppConfig of(String host, int port, String user, String password) {
        return new AppConfig(host, port, user, password);
    }
}