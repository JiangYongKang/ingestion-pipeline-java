package com.github.highcumontoa.ingestionpipelinejava.source;

import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * 拉取式本地文件来源：监听 watchDir，文件名形如 {@code 000001.json}（正整数位点）。
 * 按文件名数值升序拉取严格大于 afterOffset 的文件，位点即“最后已消费文件名数值”。
 * 断点恢复后自然跳过已确认文件；非数字文件名被忽略（不产生越位/错位）。
 */
public class FileSourceConnector implements SourceConnector {

    private final SourceConfig config;
    private final Path watchDir;

    public FileSourceConnector(SourceConfig config) {
        this.config = config;
        this.watchDir = Path.of(config.getWatchDir());
    }

    @Override
    public SourceConfig config() {
        return config;
    }

    @Override
    public boolean pullable() {
        return true;
    }

    @Override
    public PollBatch poll(long afterOffset) {
        try {
            Files.createDirectories(watchDir);
            List<Path> files = new ArrayList<>();
            try (Stream<Path> stream = Files.list(watchDir)) {
                stream.filter(Files::isRegularFile)
                        .filter(p -> parseOffset(p) != null && parseOffset(p) > afterOffset)
                        .sorted((a, b) -> Long.compare(parseOffset(a), parseOffset(b)))
                        .forEach(files::add);
            }
            List<SourceRecord> records = new ArrayList<>();
            for (Path f : files) {
                String json = Files.readString(f, StandardCharsets.UTF_8);
                records.add(new SourceRecord(parseOffset(f), json));
            }
            return new PollBatch(records);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to poll file source " + config.getId(), e);
        }
    }

    private static Long parseOffset(Path p) {
        String n = p.getFileName().toString();
        int dot = n.lastIndexOf('.');
        String prefix = dot > 0 ? n.substring(0, dot) : n;
        try {
            long v = Long.parseLong(prefix);
            return v > 0 ? v : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
