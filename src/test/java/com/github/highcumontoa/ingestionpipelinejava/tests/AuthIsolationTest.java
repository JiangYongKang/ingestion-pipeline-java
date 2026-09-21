package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.service.AuthService;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 场景：缺凭据(401)/凭据无效(401)/跨来源(403)/未知来源(404) 必须可区分。 */
class AuthIsolationTest {

    private AuthService auth() {
        PipelineProperties p = new PipelineProperties();
        SourceConfig a = new SourceConfig();
        a.setName("alpha");
        a.setType("PUSH");
        a.setToken("token-alpha");
        SourceConfig b = new SourceConfig();
        b.setName("beta");
        b.setType("PUSH");
        b.setToken("token-beta");
        p.setSources(List.of(a, b));
        return new AuthService(new SourceRegistry(p));
    }

    @Test
    void credentialFailuresAreClassifiedDistinctly() {
        AuthService auth = auth();

        var missing = org.junit.jupiter.api.Assertions.assertThrows(IngestionException.class,
                () -> auth.requireAccess("alpha", null));
        TestLog.given("auth", "no X-API-Key");
        TestLog.actual("auth-missing", missing.reason() + " (" + missing.reason().httpStatus() + ")");
        assertEquals(RejectReason.MISSING_CREDENTIAL, missing.reason());

        var invalid = org.junit.jupiter.api.Assertions.assertThrows(IngestionException.class,
                () -> auth.requireAccess("alpha", "token-nope"));
        TestLog.actual("auth-invalid", invalid.reason() + " (" + invalid.reason().httpStatus() + ")");
        assertEquals(RejectReason.INVALID_CREDENTIAL, invalid.reason());

        var cross = org.junit.jupiter.api.Assertions.assertThrows(IngestionException.class,
                () -> auth.requireAccess("alpha", "token-beta"));
        TestLog.actual("auth-cross", cross.reason() + " (" + cross.reason().httpStatus() + ")");
        assertEquals(RejectReason.CROSS_SOURCE_ACCESS, cross.reason());
        assertEquals(403, cross.reason().httpStatus().value());

        var unknown = org.junit.jupiter.api.Assertions.assertThrows(IngestionException.class,
                () -> auth.requireAccess("ghost", "token-alpha"));
        TestLog.actual("auth-unknown", unknown.reason() + " (" + unknown.reason().httpStatus() + ")");
        assertEquals(RejectReason.UNKNOWN_SOURCE, unknown.reason());

        // 正确凭据放行
        assertEquals("alpha", auth.requireAccess("alpha", "token-alpha").name());
        TestLog.because("auth", "401 与 403、404 状态码与 reason 码均不同，调用方可明确区分");
    }
}
