package com.vk.kv.repository;

/** Thrown when the Tarantool storage operation fails (connection, timeout, protocol error). */
public class StorageException extends RuntimeException {
    public StorageException(String operation, Throwable cause) {
        super("Storage operation failed: " + operation, cause);
    }
}
