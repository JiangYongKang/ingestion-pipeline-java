package com.github.highcumontoa.ingestionpipelinejava.web;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.core.AuthService;
import com.github.highcumontoa.ingestionpipelinejava.core.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.dto.IngestItem;
import com.github.highcumontoa.ingestionpipelinejava.dto.IngestRequest;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.BatchResult;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRecord;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 推送式接入端点与整批重放入口。
 *
 * <p>{@code POST /api/sources/{sourceId}/records}
 * 先鉴权（401/403，与 400 参数错误区分），未授权时绝不写入、不推进位点；
 * 通过后按位点协议处理（重复投递幂等、回退/非连续拒绝、坏数据逐条隔离）。
 */
@RestController
public class IngestController {

    private final AuthService auth;
    private final SourceRegistry registry;
    private final IngestionService ingestion;
    private final PipelineProperties props;

    public IngestController(AuthService auth, SourceRegistry registry,
                            IngestionService ingestion, PipelineProperties props) {
        this.auth = auth;
        this.registry = registry;
        this.ingestion = ingestion;
        this.props = props;
    }

    @PostMapping("/api/sources/{sourceId}/records")
    public BatchResult push(@PathVariable String sourceId,
                            @RequestHeader(value = "Authorization", required = false) String authHeader,
                            @RequestBody IngestRequest body,
                            HttpServletRequest request) {
        if (body == null || body.getRecords() == null || body.getRecords().isEmpty()) {
            request.setAttribute("sourceId", sourceId);
            throw new IngestionException(ErrorCode.PARAM_INVALID, "records must be a non-empty array");
        }
        if (body.getRecords().size() > props.getMaxBatchSize()) {
            request.setAttribute("sourceId", sourceId);
            throw new IngestionException(ErrorCode.BATCH_TOO_LARGE,
                    "batch size " + body.getRecords().size() + " exceeds configured max "
                            + props.getMaxBatchSize());
        }
        // 鉴权在任何写操作之前；失败由全局异常映射为 401/403。
        SourceConfig owner = auth.authenticate(authHeader, sourceId);
        request.setAttribute("sourceId", owner.getId());
        registry.require(sourceId);

        List<SourceRecord> records = body.getRecords().stream()
                .map(IngestController::toSourceRecord)
                .toList();
        return ingestion.ingest(sourceId, records);
    }

    private static SourceRecord toSourceRecord(IngestItem item) {
        if (item.getOffset() <= 0) {
            throw new IngestionException(ErrorCode.PARAM_INVALID, "offset must be a positive integer");
        }
        if (item.getPayload() == null || item.getPayload().isNull()) {
            throw new IngestionException(ErrorCode.PARAM_INVALID,
                    "payload is required for offset " + item.getOffset());
        }
        try {
            return new SourceRecord(item.getOffset(), JsonFiles.mapper().writeValueAsString(item.getPayload()));
        } catch (Exception e) {
            throw new IngestionException(ErrorCode.PARAM_INVALID, "payload must be valid JSON", e);
        }
    }
}
