package com.vft.cdp.common.constant;

/** Error codes per spec: "0" = success; others = failure. */
public final class ErrorCodes {
    private ErrorCodes() {
    }

    // Success
    public static final String OK = "200";

    // 4xx – client errors
    // invalid parameters
    public static final String BAD_REQUEST = "400";
    // auth required/invalid
    public static final String UNAUTHORIZED = "401";
    // no access rights
    public static final String FORBIDDEN = "403";
    // resource not found
    public static final String NOT_FOUND = "404";
    // request timeout (retryable)
    public static final String REQUEST_TIMEOUT = "408";
    // duplicate/conflict (idempotent)
    public static final String CONFLICT = "409";
    // business/logic error
    public static final String UNPROCESSABLE_ENTITY = "422";
    // rate limited (retry with backoff)
    public static final String TOO_MANY_REQUESTS = "429";

    // 5xx – server / upstream
    // internal error (retryable)
    public static final String SERVER_ERROR = "500";
    // upstream bad gateway (retryable)
    public static final String BAD_GATEWAY = "502";
    // service unavailable (retryable)
    public static final String SERVICE_UNAVAILABLE = "503";
    // upstream timeout (retryable)
    public static final String GATEWAY_TIMEOUT = "504";
    // insufficient balance for payment
    public static final String INSUFFICIENT_BALANCE = "402";
    // account locked due to overdue debt
    public static final String ACCOUNT_LOCKED_OVERDUE = "423";
    public static final int DB_OK_CODE = 200;
    public static final int DB_NOTFOUND_CODE = 404;

    // status code payment
    public static final String STATUS_PROCESSING = "PROCESSING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAIL = "FAILED";

}
