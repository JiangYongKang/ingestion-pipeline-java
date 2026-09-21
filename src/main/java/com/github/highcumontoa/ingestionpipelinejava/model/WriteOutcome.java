package com.github.highcumontoa.ingestionpipelinejava.model;

/**
 * 幂等写入的三种确定结果：
 * INSERTED 首次写入；DUPLICATE 同键同内容（含整批重放）；
 * CONFLICT 同键不同内容，不覆盖、不丢弃，转冲突处理。
 */
public enum WriteOutcome {
    INSERTED,
    DUPLICATE,
    CONFLICT
}
