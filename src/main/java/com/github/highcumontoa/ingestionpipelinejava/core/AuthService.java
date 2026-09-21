package com.github.highcumontoa.ingestionpipelinejava.core;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRegistry;

/**
 * 鉴权与来源隔离。
 *
 * <p>判定顺序与可区分原因：
 * <ol>
 *   <li>无 Authorization 头 / 非 Bearer ⇒ AUTH_MISSING_CREDENTIALS；</li>
 *   <li>令牌与任何来源都不匹配 ⇒ AUTH_INVALID_CREDENTIALS；</li>
 *   <li>令牌有效但不属于被访问来源（跨来源访问）⇒ AUTH_FORBIDDEN。</li>
 * </ol>
 * 管理端点使用独立的管理员令牌。任何鉴权失败都在写入前返回，
 * 不产生数据写入，也不改变位点。
 */
public class AuthService {

    private final SourceRegistry registry;
    private final PipelineProperties props;

    public AuthService(SourceRegistry registry, PipelineProperties props) {
        this.registry = registry;
        this.props = props;
    }

    /** 校验数据面请求，返回其归属来源配置。 */
    public SourceConfig authenticate(String authHeader, String requestedSourceId) {
        if (authHeader == null || authHeader.isBlank()
                || !authHeader.startsWith("Bearer ") || authHeader.length() <= 7) {
            throw new IngestionException(ErrorCode.AUTH_MISSING_CREDENTIALS,
                    "missing bearer token; expected 'Authorization: Bearer <token>'");
        }
        String token = authHeader.substring(7).trim();
        SourceConfig owner = null;
        for (SourceConfig cfg : registry.all()) {
            if (token.equals(cfg.getToken())) {
                owner = cfg;
                break;
            }
        }
        if (owner == null) {
            throw new IngestionException(ErrorCode.AUTH_INVALID_CREDENTIALS,
                    "token does not match any registered source");
        }
        if (!owner.getId().equals(requestedSourceId)) {
            throw new IngestionException(ErrorCode.AUTH_FORBIDDEN,
                    "token belongs to source '" + owner.getId()
                            + "' and cannot access source '" + requestedSourceId + "'");
        }
        return owner;
    }

    /** 校验管理面请求。 */
    public void requireAdmin(String authHeader) {
        String expected = props.getAdminToken();
        if (expected == null || expected.isBlank()) {
            throw new IngestionException(ErrorCode.AUTH_FORBIDDEN,
                    "admin operations are disabled (no admin token configured)");
        }
        if (authHeader == null || !authHeader.equals("Bearer " + expected)) {
            throw new IngestionException(ErrorCode.AUTH_INVALID_CREDENTIALS,
                    "invalid or missing admin token");
        }
    }
}
