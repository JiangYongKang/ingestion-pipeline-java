package com.github.highcumontoa.ingestionpipelinejava.tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 测试日志辅助：统一打印“输入”与“判定依据”，满足可复现/可解释要求。 */
public final class TestLog {

    private static final Logger log = LoggerFactory.getLogger("SCENARIO");

    private TestLog() {
    }

    public static void given(String scenario, Object input) {
        log.info("[{}] GIVEN  -> {}", scenario, input);
    }

    public static void expect(String scenario, Object expected) {
        log.info("[{}] EXPECT -> {}", scenario, expected);
    }

    public static void actual(String scenario, Object actual) {
        log.info("[{}] ACTUAL -> {}", scenario, actual);
    }

    public static void because(String scenario, String basis) {
        log.info("[{}] BECAUSE: {}", scenario, basis);
    }
}
