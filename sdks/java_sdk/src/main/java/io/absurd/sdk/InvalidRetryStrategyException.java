package io.absurd.sdk;

/**
 * Thrown when the server rejects a retry strategy at spawn time (SQLSTATE {@code AB003}).
 *
 * <p>Since schema version 0.5.0 the database validates retry strategies eagerly in
 * {@code absurd.spawn_task}: {@code base_seconds} and {@code max_seconds} must be within
 * {@code [0, 86400]} (one day), {@code factor} must be a finite non-negative number, and
 * {@code kind} must be one of {@code none}, {@code fixed}, or {@code exponential}.</p>
 */
public class InvalidRetryStrategyException extends AbsurdException {

    public InvalidRetryStrategyException(String message, Throwable cause) {
        super(message, cause);
    }
}
