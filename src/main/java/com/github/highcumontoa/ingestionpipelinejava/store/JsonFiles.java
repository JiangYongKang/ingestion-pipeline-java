package com.github.highcumontoa.ingestionpipelinejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * JSON 序列化与“临时文件 + 原子 move”落盘工具。
 * 位点等关键状态只用原子写更新，避免进程崩溃留下半截文件。
 */
public final class JsonFiles {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonFiles() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static void writeAtomically(Path target, Object value) {
        try {
            Path parent = target.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = Files.createTempFile(parent, ".tmp-", ".json");
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), value);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write " + target, e);
        }
    }

    public static <T> T read(Path file, Class<T> type) {
        try {
            return MAPPER.readValue(file.toFile(), type);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read " + file, e);
        }
    }

    public static byte[] toBytes(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
