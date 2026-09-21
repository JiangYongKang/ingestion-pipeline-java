package com.github.highcumontoa.ingestionpipelinejava.web.dto;

import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;

/** 统一错误响应：reason 为机器可区分的归类码。 */
public record ErrorResponse(RejectReason reason, String message) {
}
