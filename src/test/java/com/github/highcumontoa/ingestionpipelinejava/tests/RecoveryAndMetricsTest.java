package com.github.highcumontoa.ingestionpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.core.BacklogGuard;
import com.github.highcumontoa.ingestionpipelinejava.core.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.model.BatchResult;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaDefinition;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRecord;
import com.github.highcumontoa.ingestionpipelinejava.store.DataStore;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import com.github.highcumontoa.ingestionpipelinejava.store.QuarantineStore;
import com.github.highcumontoa.ingestionpipelinejava.model.ColumnDef;
import com.github.highcumontoa.ingestionpipelinejava.model.ColumnType;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：处理中断后“重启”——用同一数据目录重新构建全部存储/服务组件，
 * 位点、数据视图与指标必须从磁盘恢复；重放不产生重复；计数可查询。
 */
class RecoveryAndMetricsTest {

    @TempDir
    Path dir;

    private static final String SID = "r1";

    private SchemaDefinition basicSchema() {
        SchemaDefinition schema = new SchemaDefinition();
        schema.setKeyField("id");
        schema.setColumns(List.of(
                new ColumnDef("id", ColumnType.STRING, true, null),
                new ColumnDef("name", ColumnType.STRING, true, null)));
        return schema;
    }

    private IngestionService newPipeline(PipelineProperties props) {
        OffsetStore offsets = new OffsetStore(dir);
        DataStore data = new DataStore(dir);
        SchemaStore schemas = new SchemaStore(dir);
        QuarantineStore q = new QuarantineStore(dir);
        MetricsStore m = new MetricsStore(dir);
        BacklogGuard backlog = new BacklogGuard(props);
        return new IngestionService(offsets, data, schemas, q, m, backlog, props);
    }

    private PipelineProperties props() {
        PipelineProperties p = new PipelineProperties();
        p.setDataDir(dir.toString());
        p.setMaxBatchSize(100);
        p.setBacklogLimit(100);
        p.setMaxRetryAttempts(3);
        return p;
    }

    private SourceRecord rec(long offset, String id, String name) {
        ObjectNode n = JsonFiles.mapper().createObjectNode();
        n.put("id", id);
        n.put("name", name);
        return new SourceRecord(offset, n.toString());
    }

    @Test
    @DisplayName("同目录重建后位点/数据/指标延续；重放幂等；继续位点正常推进")
    void rebuildRecoversStateFromDisk() {
        PipelineProperties p1 = props();
        IngestionService first = newPipeline(p1);
        new SchemaStore(dir).initialize(SID, basicSchema());

        BatchResult b1 = first.ingest(SID, List.of(rec(1, "a", "A"), rec(2, "b", "B")));
        assertEquals(2, b1.getInserted());
        assertEquals(2, b1.getCommittedOffset());
        logInput("phase1 batch 1..2 -> inserted=2, offset=2");

        // 模拟进程结束：丢弃所有内存对象，只用磁盘上的数据目录“重启”
        IngestionService second = newPipeline(props());

        long recoveredOffset = new OffsetStore(dir).load(SID).getOffset();
        int recoveredRows = new DataStore(dir).all(SID).size();
        long insertedMetric = new MetricsStore(dir).get(SID).getInserted();
        logInput("after rebuild -> offset=" + recoveredOffset + ", rows=" + recoveredRows
                + ", insertedMetric=" + insertedMetric);
        assertEquals(2, recoveredOffset, "位点必须从磁盘恢复");
        assertEquals(2, recoveredRows, "数据视图必须从快照/WAL 恢复");
        assertEquals(2, insertedMetric, "指标计数必须从磁盘延续");

        // 整批重放：全部 DUPLICATE，行数不变
        BatchResult replay = second.ingest(SID, List.of(rec(1, "a", "A"), rec(2, "b", "B")));
        assertEquals(2, replay.getDuplicates());
        assertEquals(2, replay.getCommittedOffset(), "重放不改变位点");
        assertEquals(2, new DataStore(dir).all(SID).size(), "重放不产生重复行");

        // 从位点 3 继续
        BatchResult cont = second.ingest(SID, List.of(rec(3, "c", "C")));
        assertEquals(1, cont.getInserted());
        assertEquals(3, cont.getCommittedOffset());

        MetricsStore finalMetrics = new MetricsStore(dir);
        var m = finalMetrics.get(SID);
        logInput("[OBSERVABLE] received=" + m.getReceived() + " inserted=" + m.getInserted()
                + " duplicates=" + m.getDuplicates() + " conflicts=" + m.getConflicts()
                + " quarantined=" + m.getQuarantined() + " rejected=" + m.getRejected()
                + " retried=" + m.getRetried());
        assertEquals(5, m.getReceived());
        assertEquals(3, m.getInserted());
        assertEquals(2, m.getDuplicates());
        assertTrue(new DataStore(dir).all(SID).size() == 3, "最终数据一致：3 行");
    }

    private void logInput(String s) {
        org.slf4j.LoggerFactory.getLogger(RecoveryAndMetricsTest.class).info("[INPUT/JUDGE] {}", s);
    }
}
