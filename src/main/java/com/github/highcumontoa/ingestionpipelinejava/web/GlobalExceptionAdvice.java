package com.github.highcumontoa.ingestionpipelinejava.web;

import com.github.highcumontoa.ingestionpipelinejava.dto.ErrorResponse;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一异常映射。错误类别 → HTTP 状态：
 * 鉴权类 401/403，来源/参数/批量类 400/404，位点与积压类 409/429。
 * 所有错误都在写入前或处理边界抛出，处理体内未提交的数据不会落盘。
 * rejected 计数仅为可观测统计，不代表任何数据写入或位点变化。
 */
@RestControllerAdvice
public class GlobalExceptionAdvice {

    private final MetricsStore metrics;

    public GlobalExceptionAdvice(MetricsStore metrics) {
        this.metrics = metrics;
    }

    @ExceptionHandler(IngestionException.class)
    public ResponseEntity<ErrorResponse> handle(IngestionException ex, HttpServletRequest req) {
        countReject(req, ex.getCode());
        return ResponseEntity.status(status(ex.getCode()))
                .body(new ErrorResponse(ex.getCode(), ex.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex,
                                                         HttpServletRequest req) {
        countReject(req, ErrorCode.PARAM_INVALID);
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(ErrorCode.PARAM_INVALID, "malformed request body: "
                        + ex.getMostSpecificCause().getMessage()));
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingHeader(MissingRequestHeaderException ex) {
        // 缺少 Authorization 头本质是缺凭据，归 401 而非 500。
        if ("Authorization".equalsIgnoreCase(ex.getHeaderName())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(new ErrorResponse(ErrorCode.AUTH_MISSING_CREDENTIALS,
                            "missing required Authorization header"));
        }
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(ErrorCode.PARAM_INVALID, ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleOther(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(ErrorCode.PARAM_INVALID, "unexpected error: " + ex.getMessage()));
    }

    private void countReject(HttpServletRequest req, ErrorCode code) {
        String sourceId = (String) req.getAttribute("sourceId");
        if (sourceId != null) {
            try {
                metrics.increment(sourceId, m -> m.setRejected(m.getRejected() + 1));
            } catch (Exception ignored) {
                // 统计失败不影响错误响应
            }
        }
    }

    static HttpStatus status(ErrorCode code) {
        return switch (code) {
            case AUTH_MISSING_CREDENTIALS, AUTH_INVALID_CREDENTIALS -> HttpStatus.UNAUTHORIZED;
            case AUTH_FORBIDDEN -> HttpStatus.FORBIDDEN;
            case SOURCE_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case BACKLOG_LIMIT_REACHED -> HttpStatus.TOO_MANY_REQUESTS;
            case OFFSET_FALLBACK, OFFSET_NON_MONOTONIC, OFFSET_VERSION_STALE,
                 CONFLICT, SOURCE_DISABLED -> HttpStatus.CONFLICT;
            default -> HttpStatus.BAD_REQUEST;
        };
    }
}
