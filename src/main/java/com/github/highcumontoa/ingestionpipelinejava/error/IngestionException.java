package com.github.highcumontoa.ingestionpipelinejava.error;

import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;

/**
 * 处理过程中的可解释拒绝/失败。携带稳定错误码与可读原因，
 * 用于在 HTTP 响应、隔离记录与测试日志中给出判定依据。
 */
public class IngestionException extends RuntimeException {

    private final ErrorCode code;

    public IngestionException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public IngestionException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
