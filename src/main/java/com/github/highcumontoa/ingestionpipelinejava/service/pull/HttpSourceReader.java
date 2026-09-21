package com.github.highcumontoa.ingestionpipelinejava.service.pull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HTTP 拉取源：GET {baseUrl}/records?after={n}&limit={m}，请求头携带 X-API-Key。
 * 要求响应记录按 sequence 升序且从 after+1 连续，否则 SOURCE_UNAVAILABLE（拒绝错位数据）。
 */
public class HttpSourceReader implements SourceReader {

    private final String baseUrl;
    private final String token;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    public HttpSourceReader(String baseUrl, String token) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.token = token;
    }

    @Override
    public List<PulledRecord> read(long fromExclusive, int maxRecords) {
        String url = baseUrl + "/records?after=" + fromExclusive + "&limit=" + maxRecords;
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("X-API-Key", token)
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> resp;
        try {
            resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new IngestionException(RejectReason.SOURCE_UNAVAILABLE,
                    "http source unavailable: " + url + " (" + e.getMessage() + ")", e);
        }
        if (resp.statusCode() / 100 != 2) {
            throw new IngestionException(RejectReason.SOURCE_UNAVAILABLE,
                    "http source returned status " + resp.statusCode() + " for " + url);
        }
        Map<String, Object> body;
        try {
            body = JsonFiles.mapper().readValue(resp.body(),
                    new TypeReference<Map<String, Object>>() {
                    });
        } catch (Exception e) {
            throw new IngestionException(RejectReason.SOURCE_UNAVAILABLE,
                    "http source returned unparseable body: " + e.getMessage(), e);
        }
        Object rawRecords = body.get("records");
        if (!(rawRecords instanceof List<?> list)) {
            return List.of();
        }
        List<PulledRecord> out = new ArrayList<>();
        long expected = fromExclusive + 1;
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> rm)) {
                throw new IngestionException(RejectReason.SOURCE_UNAVAILABLE,
                        "http source record is not an object");
            }
            Object seqObj = rm.get("sequence");
            long seq;
            try {
                seq = ((Number) seqObj).longValue();
            } catch (Exception e) {
                throw new IngestionException(RejectReason.SOURCE_UNAVAILABLE,
                        "http source record missing numeric 'sequence'");
            }
            if (seq != expected) {
                throw new IngestionException(RejectReason.SOURCE_UNAVAILABLE,
                        "http source non-contiguous sequence: expected " + expected + " got " + seq);
            }
            Object key = rm.get("key");
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) rm.get("data");
            if (key == null || data == null) {
                out.add(PulledRecord.malformed(seq, rm.toString(), "missing 'key' or 'data'"));
            } else {
                out.add(PulledRecord.ok(seq, new IncomingRecord(key.toString(), data)));
            }
            expected++;
        }
        return out;
    }
}
