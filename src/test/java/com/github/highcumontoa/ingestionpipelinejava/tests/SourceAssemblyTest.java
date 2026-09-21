package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 场景：类型未知与配置不完整必须被明确拒绝，且原因码可区分。 */
class SourceAssemblyTest {

    private PipelineProperties props() {
        PipelineProperties p = new PipelineProperties();
        return p;
    }

    private SourceConfig base(String name, String type) {
        SourceConfig c = new SourceConfig();
        c.setName(name);
        c.setType(type);
        c.setToken("tok-" + name);
        return c;
    }

    @Test
    void unknownTypeIsRejectedWithDistinctReason() {
        String scenario = "assembly-unknown-type";
        SourceConfig c = base("s1", "KAFKA");
        TestLog.given(scenario, "type=KAFKA (unsupported)");
        IngestionException ex = assertThrows(IngestionException.class,
                () -> SourceRegistry.validate(c, props(), new HashSet<>()));
        TestLog.actual(scenario, ex.reason() + " | " + ex.getMessage());
        assertEquals(RejectReason.UNKNOWN_SOURCE_TYPE, ex.reason());
        TestLog.because(scenario, "未知类型必须归类 UNKNOWN_SOURCE_TYPE，而非通用参数错误");
    }

    @Test
    void incompleteConfigIsRejectedWithDistinctReason() {
        String scenario = "assembly-incomplete-config";
        SourceConfig c = base("s2", "PULL_FILE"); // 缺 dir
        c.setDir(null);
        TestLog.given(scenario, "type=PULL_FILE without dir");
        IngestionException ex = assertThrows(IngestionException.class,
                () -> SourceRegistry.validate(c, props(), new HashSet<>()));
        TestLog.actual(scenario, ex.reason() + " | " + ex.getMessage());
        assertEquals(RejectReason.INCOMPLETE_CONFIG, ex.reason());

        SourceConfig noToken = base("s3", "PUSH");
        noToken.setToken("  ");
        IngestionException ex2 = assertThrows(IngestionException.class,
                () -> SourceRegistry.validate(noToken, props(), new HashSet<>()));
        TestLog.actual(scenario + "-no-token", ex2.reason() + " | " + ex2.getMessage());
        assertEquals(RejectReason.INCOMPLETE_CONFIG, ex2.reason());
        TestLog.because(scenario, "类型合法但必需项缺失 -> INCOMPLETE_CONFIG，与 UNKNOWN_SOURCE_TYPE 可区分");
    }

    @Test
    void registryRejectsDuplicateNameAndUnknownLookup() {
        String scenario = "assembly-registry";
        PipelineProperties p = props();
        SourceConfig a = base("dup", "PUSH");
        SourceConfig b = base("dup", "PUSH");
        p.setSources(List.of(a, b));
        TestLog.given(scenario, "two sources with same name 'dup'");
        IngestionException ex = assertThrows(IngestionException.class, () -> new SourceRegistry(p));
        TestLog.actual(scenario, ex.reason() + " | " + ex.getMessage());
        assertEquals(RejectReason.INCOMPLETE_CONFIG, ex.reason());

        PipelineProperties ok = props();
        ok.setSources(List.of(base("only", "PUSH")));
        SourceRegistry reg = new SourceRegistry(ok);
        IngestionException unknown = assertThrows(IngestionException.class, () -> reg.require("nope"));
        TestLog.actual(scenario + "-lookup", unknown.reason() + " | " + unknown.getMessage());
        assertEquals(RejectReason.UNKNOWN_SOURCE, unknown.reason());
        assertTrue(unknown.reason().httpStatus().value() == 404);
        TestLog.because(scenario, "重名=配置错误(400)；运行期未知来源=UNKNOWN_SOURCE(404)");
    }
}
