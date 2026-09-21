package com.github.highcumontoa.ingestionpipelinejava.store;

import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ColumnDef;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaDefinition;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 结构定义的持久化与受控演进。
 *
 * <p>允许的演进：新增列（必须给出确定的 defaultValue）、删除列。
 * 不允许：删除业务键列、改变既有列类型（破坏已存数据的解释）。
 * 每次演进产生新版本号，历史版本保留以便解释旧数据。
 */
public class SchemaStore {

    private final Path root;

    public SchemaStore(Path dataDir) {
        this.root = dataDir.resolve("schemas");
    }

    private Path file(String sourceId) {
        return root.resolve(sourceId + ".json");
    }

    public boolean exists(String sourceId) {
        return Files.exists(file(sourceId));
    }

    public SchemaState load(String sourceId) {
        if (!exists(sourceId)) {
            return null;
        }
        return JsonFiles.read(file(sourceId), SchemaState.class);
    }

    public SchemaDefinition current(String sourceId) {
        SchemaState s = load(sourceId);
        return s == null ? null : s.getCurrent();
    }

    /** 首次注册结构。 */
    public synchronized SchemaDefinition initialize(String sourceId, SchemaDefinition schema) {
        if (exists(sourceId)) {
            throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                    "schema already initialized for source '" + sourceId + "'; use evolve");
        }
        validateShape(schema);
        schema.setVersion(1);
        SchemaState state = new SchemaState();
        state.setCurrent(schema);
        state.setHistory(new ArrayList<>(List.of(clone(schema))));
        JsonFiles.writeAtomically(file(sourceId), state);
        return schema;
    }

    /**
     * 受控演进。target 描述新结构；与当前版本对比推导新增/删除列。
     */
    public synchronized SchemaDefinition evolve(String sourceId, SchemaDefinition target) {
        SchemaState state = load(sourceId);
        if (state == null) {
            throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                    "schema not initialized for source '" + sourceId + "'; use register first");
        }
        validateShape(target);
        SchemaDefinition current = state.getCurrent();
        if (!current.getKeyField().equals(target.getKeyField())) {
            throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                    "key field is immutable for source '" + sourceId + "': " + current.getKeyField());
        }
        java.util.Map<String, ColumnDef> oldByName = new java.util.LinkedHashMap<>();
        current.getColumns().forEach(c -> oldByName.put(c.getName(), c));
        java.util.Map<String, ColumnDef> newByName = new java.util.LinkedHashMap<>();
        target.getColumns().forEach(c -> newByName.put(c.getName(), c));

        List<String> removed = new ArrayList<>();
        for (String name : oldByName.keySet()) {
            if (!newByName.containsKey(name)) {
                if (name.equals(current.getKeyField())) {
                    throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                            "cannot remove key field '" + name + "' for source '" + sourceId + "'");
                }
                removed.add(name);
            }
        }
        for (ColumnDef nc : target.getColumns()) {
            ColumnDef old = oldByName.get(nc.getName());
            if (old != null && old.getType() != nc.getType()) {
                throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                        "changing type of column '" + nc.getName() + "' is not allowed for source '"
                                + sourceId + "'; remove and add as a new column instead");
            }
            if (old == null && nc.getDefaultValue() == null && !nc.isRequired()) {
                throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                        "newly added column '" + nc.getName()
                                + "' must declare an explicit defaultValue for stable semantics");
            }
        }
        target.setVersion(current.getVersion() + 1);
        List<SchemaDefinition> history = new ArrayList<>(state.getHistory());
        history.add(clone(target));
        state.setCurrent(target);
        state.setHistory(history);
        List<String> removedAll = new ArrayList<>(state.getRemovedColumns());
        removedAll.removeAll(newByName.keySet());
        for (String r : removed) {
            if (!removedAll.contains(r)) {
                removedAll.add(r);
            }
        }
        state.setRemovedColumns(removedAll);
        JsonFiles.writeAtomically(file(sourceId), state);
        return target;
    }

    private static void validateShape(SchemaDefinition schema) {
        if (schema == null || schema.getKeyField() == null || schema.getKeyField().isBlank()) {
            throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID, "keyField is required");
        }
        if (schema.getColumns() == null || schema.getColumns().isEmpty()) {
            throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID, "columns must not be empty");
        }
        boolean hasKey = schema.getColumns().stream().anyMatch(c -> c.getName().equals(schema.getKeyField()));
        if (!hasKey) {
            throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                    "key field '" + schema.getKeyField() + "' must be declared in columns");
        }
    }

    private static SchemaDefinition clone(SchemaDefinition s) {
        SchemaDefinition copy = new SchemaDefinition();
        copy.setVersion(s.getVersion());
        copy.setKeyField(s.getKeyField());
        List<ColumnDef> cols = new ArrayList<>();
        for (ColumnDef c : s.getColumns()) {
            cols.add(new ColumnDef(c.getName(), c.getType(), c.isRequired(), c.getDefaultValue()));
        }
        copy.setColumns(cols);
        return copy;
    }
}
