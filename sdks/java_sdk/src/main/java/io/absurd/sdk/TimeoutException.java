package io.absurd.sdk;

/**
 * Thrown when awaiting an event or task result runs into a timeout.
 */
public final class TimeoutException extends AbsurdException {

    public TimeoutException(String message) {
        super(message);
    }
}
