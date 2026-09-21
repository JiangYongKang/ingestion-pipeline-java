package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 场景：位点回退/缺口拒绝；并发串行推进不跳变；重启后从已确认位置恢复。 */
class OffsetStoreTest {

    private OffsetStore store(Path tmp) {
        PipelineProperties p = new PipelineProperties();
        p.setStateDir(tmp);
        return new OffsetStore(p);
    }

    @Test
    void backwardAndGapAreRejectedWithDistinctReasons(@TempDir Path tmp) {
        String scenario = "offset-guard";
        OffsetStore s = store(tmp);
        s.commitContiguous("src", 0, 1, true);
        s.commitContiguous("src", 1, 2, true);
        TestLog.given(scenario, "committed 0->1->2; try backward to 1, then jump 2->5");

        IngestionException back = assertThrows(IngestionException.class,
                () -> s.commitContiguous("src", 2, 1, true));
        TestLog.actual(scenario + "-backward", back.reason() + " | " + back.getMessage());
        assertEquals(RejectReason.OFFSET_BACKWARD, back.reason());

        IngestionException gap = assertThrows(IngestionException.class,
                () -> s.commitContiguous("src", 2, 5, true));
        TestLog.actual(scenario + "-gap", gap.reason() + " | " + gap.getMessage());
        assertEquals(RejectReason.OFFSET_GAP, gap.reason());

        IngestionException stale = assertThrows(IngestionException.class,
                () -> s.commitContiguous("src", 1, 2, true));
        TestLog.actual(scenario + "-stale-expected", stale.reason() + " | " + stale.getMessage());
        assertEquals(RejectReason.OFFSET_BACKWARD, stale.reason());
        TestLog.because(scenario, "回退/陈旧 expected -> OFFSET_BACKWARD；跳跃 -> OFFSET_GAP，二者可区分");
    }

    @Test
    void concurrentAdvanceIsSerializedAndNeverJumps(@TempDir Path tmp) throws Exception {
        String scenario = "offset-concurrency";
        OffsetStore s = store(tmp);
        int threads = 16;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger rejected = new AtomicInteger();
        TestLog.given(scenario, threads + " threads each attempting " + perThread
                + " contiguous commits on the same source");

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < perThread; i++) {
                    while (true) {
                        long cur = s.current("src");
                        try {
                            s.commitContiguous("src", cur, cur + 1, true);
                            break;
                        } catch (IngestionException ex) {
                            rejected.incrementAndGet(); // 竞争失败者重试，绝不允许跳变
                        }
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long finalOffset = s.current("src");
        TestLog.actual(scenario, "finalOffset=" + finalOffset + " contendedRetries=" + rejected.get());
        assertEquals((long) threads * perThread, finalOffset, "成功提交必须恰好等于总推进数，无跳变/越位");
        TestLog.because(scenario, "每来源一把锁串行化提交，失败方以最新 expected 重试");
    }

    @Test
    void offsetSurvivesRestart(@TempDir Path tmp) {
        String scenario = "offset-restart";
        OffsetStore first = store(tmp);
        first.commitContiguous("src", 0, 1, true);
        first.commitContiguous("src", 1, 7, false); // 非严格模式允许批量推进
        TestLog.given(scenario, "process A commits up to 7 then stops; new store instance reads disk");

        OffsetStore restarted = store(tmp);
        long resumed = restarted.current("src");
        TestLog.actual(scenario, "resumed offset=" + resumed);
        assertEquals(7, resumed);
        IngestionException back = assertThrows(IngestionException.class,
                () -> restarted.commitContiguous("src", 7, 6, true));
        assertEquals(RejectReason.OFFSET_BACKWARD, back.reason());
        TestLog.because(scenario, "位点原子落盘，重启后从 7 继续，回退仍然被拒");
    }
}
