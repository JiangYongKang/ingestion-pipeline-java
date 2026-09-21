package com.github.highcumontoa.ingestionpipelinejava.model;

/** 携带可区分原因的业务异常，由全局异常处理器统一映射为 HTTP 响应。 */
public class IngestionException extends RuntimeException {
    private final RejectReason reason;

    public IngestionException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public IngestionException(RejectReason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
