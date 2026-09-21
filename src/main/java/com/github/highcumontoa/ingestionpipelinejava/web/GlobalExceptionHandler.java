package com.github.highcumontoa.ingestionpipelinejava.web;

import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.web.dto.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一错误映射，保证调用方可以按 reason 区分：
 * 业务原因 -> 其约定 HTTP 状态；请求体不可解析 -> MALFORMED_BODY(400)；
 * 未预期错误 -> 500（不伪装成数据/鉴权原因）。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(IngestionException.class)
    public ResponseEntity<ErrorResponse> handle(IngestionException ex) {
        return ResponseEntity.status(ex.reason().httpStatus())
                .body(new ErrorResponse(ex.reason(), ex.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.status(RejectReason.MALFORMED_BODY.httpStatus())
                .body(new ErrorResponse(RejectReason.MALFORMED_BODY,
                        "request body is not valid JSON: " + ex.getMostSpecificCause().getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleOther(Exception ex) {
        log.error("unexpected error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(RejectReason.SOURCE_UNAVAILABLE,
                        "internal error: " + ex.getClass().getSimpleName()));
    }
}
