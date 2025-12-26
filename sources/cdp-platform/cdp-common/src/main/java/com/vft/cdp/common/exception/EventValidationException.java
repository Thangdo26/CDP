package com.vft.cdp.common.exception;

/**
 * Exception dùng để báo lỗi validate dữ liệu IngestionEvent.
 * Những lỗi này là lỗi nghiệp vụ (Bad Data) và thường không cần retry.
 */
public class EventValidationException extends RuntimeException {

    public EventValidationException(String message) {
        super(message);
    }

    public EventValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventValidationException(Throwable cause) {
        super(cause);
    }
}
