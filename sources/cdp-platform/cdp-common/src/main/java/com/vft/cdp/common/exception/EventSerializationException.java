package com.vft.cdp.common.exception;

/**
 * Lỗi xảy ra khi serialize/deserialze event (JSON, Avro, ...).
 */
public class EventSerializationException extends CdpException {

    private static final long serialVersionUID = 1L;

    public EventSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
