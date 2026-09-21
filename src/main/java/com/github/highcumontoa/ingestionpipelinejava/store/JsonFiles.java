package com.github.highcumontoa.ingestionpipelinejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** JSON 读写、原子落盘与 JSONL 有界读取工具（无状态）。 */
public final class JsonFiles {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private JsonFiles() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /** 读取 JSON 文件，文件不存在返回 null。 */
    public static <T> T read(Path file, Class<T> type) {
        if (!Files.exists(file)) {
            return null;
        }
        try {
            return MAPPER.readValue(file.toFile(), type);
        } catch (IOException e) {
            throw new UncheckedIOException("read json failed: " + file, e);
        }
    }

    /** 读取 JSON 文件并映射为通用类型（如 List<Foo>），文件不存在返回 null。 */
    public static <T> T read(Path file, com.fasterxml.jackson.core.type.TypeReference<T> type) {
        if (!Files.exists(file)) {
            return null;
        }
        try {
            return MAPPER.readValue(file.toFile(), type);
        } catch (IOException e) {
            throw new UncheckedIOException("read json failed: " + file, e);
        }
    }

    /** 原子写入：先写同目录临时文件再 move，保证落盘内容完整可见。 */
    public static void writeAtomically(Path file, Object value) {
        try {
            Files.createDirectories(file.getParent());
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            MAPPER.writeValue(tmp.toFile(), value);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("write json failed: " + file, e);
        }
    }

    /** 追加一行 JSON（JSONL）。 */
    public static void appendJsonl(Path file, Object value) {
        try {
            Files.createDirectories(file.getParent());
            try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)) {
                w.write(MAPPER.writeValueAsString(value));
                w.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("append jsonl failed: " + file, e);
        }
    }

    /**
     * 有界读取 JSONL：跳过前 skipLines 行，最多读取 maxLines 行。
     * 逐行解析，不把整个文件载入内存；坏行交由 lineParser 决定如何处理。
     */
    public static <T> List<T> readJsonl(Path file, long skipLines, int maxLines,
                                        Function<String, T> lineParser) {
        List<T> out = new ArrayList<>();
        if (!Files.exists(file) || maxLines <= 0) {
            return out;
        }
        long skipped = 0;
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                if (skipped < skipLines) {
                    skipped++;
                    continue;
                }
                if (out.size() >= maxLines) {
                    break;
                }
                if (!line.isBlank()) {
                    out.add(lineParser.apply(line));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("read jsonl failed: " + file, e);
        }
        return out;
    }
}
