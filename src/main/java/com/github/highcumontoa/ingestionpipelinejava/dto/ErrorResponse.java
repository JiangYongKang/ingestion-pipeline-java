package com.github.highcumontoa.ingestionpipelinejava.dto;

import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;

/**
 * 统一错误响应体：稳定错误码 + 可读原因，保证拒绝原因可区分、可复现。
 */
public class ErrorResponse {

    private ErrorCode code;
    private String message;

    public ErrorResponse() {
    }

    public ErrorResponse(ErrorCode code, String message) {
        this.code = code;
        this.message = message;
    }

    public ErrorCode getCode() { return code; }
    public void setCode(ErrorCode code) { this.code = code; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
