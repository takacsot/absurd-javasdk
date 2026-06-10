package io.absurd.sdk;

public class AbsurdException extends RuntimeException {

    public AbsurdException(String message) {
        super(message);
    }

    public AbsurdException(String message, Throwable cause) {
        super(message, cause);
    }
}
