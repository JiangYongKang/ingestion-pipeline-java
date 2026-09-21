package com.github.highcumontoa.ingestionpipelinejava.service.pull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 文件拉取源：读取本地目录下 records-*.jsonl（JSON Lines，每行一条
 * {"key":"...","data":{...}}），文件名升序拼接为连续序列，行号即位点（从 1 起）。
 * 有界：每次最多读取 maxRecords 行；通过行跳过定位，不整文件载入内存。
 */
public class FileSourceReader implements SourceReader {

    private final Path dir;

    public FileSourceReader(Path dir) {
        this.dir = dir;
    }

    private List<Path> files() {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().startsWith("records-")
                            && p.getFileName().toString().endsWith(".jsonl"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
        } catch (Exception e) {
            return List.of();
        }
    }

    @Override
    public List<PulledRecord> read(long fromExclusive, int maxRecords) {
        List<PulledRecord> out = new ArrayList<>();
        long globalLine = 0;
        long toSkip = fromExclusive;
        for (Path file : files()) {
            if (out.size() >= maxRecords) {
                break;
            }
            try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = r.readLine()) != null) {
                    globalLine++;
                    if (toSkip > 0) {
                        toSkip--;
                        continue;
                    }
                    if (out.size() >= maxRecords) {
                        break;
                    }
                    if (line.isBlank()) {
                        continue;
                    }
                    out.add(parse(globalLine, line));
                }
            } catch (Exception e) {
                throw new IllegalStateException("file source read failed: " + file, e);
            }
        }
        return out;
    }

    private PulledRecord parse(long sequence, String line) {
        try {
            Map<String, Object> raw = JsonFiles.mapper().readValue(line,
                    new TypeReference<Map<String, Object>>() {
                    });
            Object key = raw.get("key");
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) raw.get("data");
            if (key == null || data == null) {
                return PulledRecord.malformed(sequence, line, "missing 'key' or 'data'");
            }
            return PulledRecord.ok(sequence, new IncomingRecord(key.toString(), data));
        } catch (Exception e) {
            return PulledRecord.malformed(sequence, line, e.getMessage());
        }
    }
}
