package com.github.highcumontoa.ingestionpipelinejava.model;

import java.util.Map;

/** 单条输入记录：业务键 + 原始内容（保序 LinkedHashMap 由 JSON 反序列化提供）。 */
public record IncomingRecord(String key, Map<String, Object> data) {
}
