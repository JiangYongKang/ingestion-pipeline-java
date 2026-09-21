package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import org.springframework.stereotype.Component;

/**
 * 来源鉴权与隔离。所有数据接口在发生任何写入/位点变更之前先过此校验：
 * <ul>
 *   <li>未带凭据 -> MISSING_CREDENTIAL（401）；</li>
 *   <li>凭据不属于任何来源 -> INVALID_CREDENTIAL（401）；</li>
 *   <li>凭据有效但属于其他来源 -> CROSS_SOURCE_ACCESS（403），与参数错误（4xx 400 类）可区分；</li>
 *   <li>来源不存在 -> UNKNOWN_SOURCE（404）。</li>
 * </ul>
 */
@Component
public class AuthService {

    private final SourceRegistry registry;

    public AuthService(SourceRegistry registry) {
        this.registry = registry;
    }

    /** 校验对某个来源的访问权，失败即抛异常（无副作用）。 */
    public SourceDefinition requireAccess(String source, String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IngestionException(RejectReason.MISSING_CREDENTIAL,
                    "missing credential: header X-API-Key is required");
        }
        SourceDefinition target = registry.require(source);
        if (apiKey.equals(target.token())) {
            return target;
        }
        for (SourceDefinition other : registry.all()) {
            if (!other.name().equals(source) && apiKey.equals(other.token())) {
                throw new IngestionException(RejectReason.CROSS_SOURCE_ACCESS,
                        "credential belongs to source '" + other.name()
                                + "' and cannot access source '" + source + "'");
            }
        }
        throw new IngestionException(RejectReason.INVALID_CREDENTIAL,
                "invalid credential for source '" + source + "'");
    }

    /**
     * 管理面全局入口：凭据本身必须有效（属于某个已装配来源）。
     * 返回其归属来源，用于把可见数据隔离在该来源范围内。
     */
    public SourceDefinition requireAnyValid(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IngestionException(RejectReason.MISSING_CREDENTIAL,
                    "missing credential: header X-API-Key is required");
        }
        for (SourceDefinition def : registry.all()) {
            if (apiKey.equals(def.token())) {
                return def;
            }
        }
        throw new IngestionException(RejectReason.INVALID_CREDENTIAL, "invalid credential");
    }
}
