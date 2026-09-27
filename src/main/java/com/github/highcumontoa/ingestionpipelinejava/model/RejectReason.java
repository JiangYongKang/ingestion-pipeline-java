package com.github.highcumontoa.ingestionpipelinejava.model;

import org.springframework.http.HttpStatus;

/**
 * 拒绝/失败原因的可区分归类。鉴权类 401/403、参数类 400、
 * 积压类 429、数据类 422/409，保证调用方可以据码区分。
 */
public enum RejectReason {
    UNKNOWN_SOURCE_TYPE(HttpStatus.BAD_REQUEST),
    INCOMPLETE_CONFIG(HttpStatus.BAD_REQUEST),
    UNKNOWN_SOURCE(HttpStatus.NOT_FOUND),
    MISSING_CREDENTIAL(HttpStatus.UNAUTHORIZED),
    INVALID_CREDENTIAL(HttpStatus.UNAUTHORIZED),
    CROSS_SOURCE_ACCESS(HttpStatus.FORBIDDEN),
    MALFORMED_BODY(HttpStatus.BAD_REQUEST),
    MISSING_BATCH_ID(HttpStatus.BAD_REQUEST),
    BATCH_TOO_LARGE(HttpStatus.PAYLOAD_TOO_LARGE),
    RATE_LIMITED(HttpStatus.TOO_MANY_REQUESTS),
    BACKLOG_FULL(HttpStatus.TOO_MANY_REQUESTS),
    GLOBAL_CAPACITY_EXHAUSTED(HttpStatus.TOO_MANY_REQUESTS),
    INVALID_QUOTA(HttpStatus.BAD_REQUEST),
    OFFSET_BACKWARD(HttpStatus.CONFLICT),
    OFFSET_GAP(HttpStatus.CONFLICT),
    OFFSET_FROZEN(HttpStatus.CONFLICT),
    SCHEMA_VERSION_INVALID(HttpStatus.UNPROCESSABLE_ENTITY),
    MISSING_FIELD(HttpStatus.UNPROCESSABLE_ENTITY),
    TYPE_MISMATCH(HttpStatus.UNPROCESSABLE_ENTITY),
    COLUMN_DROPPED(HttpStatus.UNPROCESSABLE_ENTITY),
    UNKNOWN_FIELD(HttpStatus.UNPROCESSABLE_ENTITY),
    RECORD_CONFLICT(HttpStatus.CONFLICT),
    SOURCE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE);

    private final HttpStatus httpStatus;

    RejectReason(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public HttpStatus httpStatus() {
        return httpStatus;
    }
}
