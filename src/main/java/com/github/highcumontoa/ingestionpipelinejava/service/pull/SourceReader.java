package com.github.highcumontoa.ingestionpipelinejava.service.pull;

import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import java.util.List;

/** 拉取式来源读取抽象：从 fromExclusive（不含）顺序读取，最多 maxRecords 条。 */
public interface SourceReader {

    List<PulledRecord> read(long fromExclusive, int maxRecords);

    /**
     * 拉取到的一条。parseError 非空表示源端该行无法解析（坏数据），
     * record 可能为 null；由调度器隔离并保留位点连续性。
     */
    record PulledRecord(long sequence, IncomingRecord record, String parseError) {

        public static PulledRecord ok(long sequence, IncomingRecord record) {
            return new PulledRecord(sequence, record, null);
        }

        public static PulledRecord malformed(long sequence, String rawLine, String error) {
            return new PulledRecord(sequence, null, "unparseable line: " + error + "; raw=" + rawLine);
        }
    }
}
