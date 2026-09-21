package com.github.highcumontoa.ingestionpipelinejava.web;

import com.github.highcumontoa.ingestionpipelinejava.model.BatchResult;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordBatch;
import com.github.highcumontoa.ingestionpipelinejava.service.AuthService;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 推送式接入口。鉴权在任何写入之前完成：
 * POST /api/sources/{source}/ingest，X-API-Key 凭据，按来源隔离。
 */
@RestController
@RequestMapping("/api/sources/{source}")
public class IngestionController {

    private final AuthService auth;
    private final IngestionService ingestionService;

    public IngestionController(AuthService auth, IngestionService ingestionService) {
        this.auth = auth;
        this.ingestionService = ingestionService;
    }

    @PostMapping("/ingest")
    public BatchResult ingest(@PathVariable String source,
                              @RequestHeader(value = "X-API-Key", required = false) String apiKey,
                              @RequestBody(required = false) RecordBatch batch) {
        // 先鉴权（未知来源/缺凭据/凭据无效/跨来源），全部无写入副作用
        auth.requireAccess(source, apiKey);
        return ingestionService.ingestBatch(source, batch);
    }
}
