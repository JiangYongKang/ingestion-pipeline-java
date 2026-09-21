package com.github.highcumontoa.ingestionpipelinejava.dto;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 推送/批量重放请求中的单条：单调位点 + 记录内容（任意 JSON 对象）。
 */
public class IngestItem {

    private long offset;
    private JsonNode payload;

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public JsonNode getPayload() { return payload; }
    public void setPayload(JsonNode payload) { this.payload = payload; }
}
