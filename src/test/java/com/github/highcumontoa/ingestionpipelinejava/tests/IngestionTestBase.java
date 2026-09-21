package com.github.highcumontoa.ingestionpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * 集成测试基类：随机端口 + 独立临时数据目录 + 管理端辅助操作。
 * 每个具体测试类在自己的 data-dir 下运行，互不污染。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ingestion.admin-token=test-admin",
                "ingestion.pull-scheduler-enabled=false",
                "ingestion.max-batch-size=50",
                "ingestion.backlog-limit=25",
                "ingestion.max-retry-attempts=3"
        })
public abstract class IngestionTestBase {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @LocalServerPort
    protected int port;

    @Autowired
    protected TestRestTemplate rest;

    private static final Path TEMP_ROOT;
    protected final Path dataDir;

    static {
        try {
            TEMP_ROOT = Files.createTempDirectory("ingestion-it-");
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected IngestionTestBase() {
        try {
            this.dataDir = Files.createDirectory(
                    TEMP_ROOT.resolve(getClass().getSimpleName() + "-" + System.nanoTime()));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void dataDirProperty(DynamicPropertyRegistry registry) throws IOException {
        // 每个测试类共享其类级上下文目录；具体测试可在此目录下再隔离。
        Path dir = Files.createTempDirectory(TEMP_ROOT, "ctx-");
        registry.add("ingestion.data-dir", dir::toString);
    }

    protected String base() {
        return "http://127.0.0.1:" + port;
    }

    protected HttpHeaders adminHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setBearerAuth("test-admin");
        return h;
    }

    protected HttpHeaders sourceHeaders(String token) {
        HttpHeaders h = new HttpHeaders();
        h.setBearerAuth(token);
        return h;
    }

    /** 动态注册一个来源。 */
    protected JsonNode registerSource(String id, String type, String token, Map<String, String> extra) {
        ObjectNode body = JsonFiles.mapper().createObjectNode();
        body.put("id", id);
        body.put("type", type);
        body.put("token", token);
        body.put("keyField", "id");
        if (extra != null) {
            extra.forEach(body::put);
        }
        log.info("[SETUP] register source: {}", body);
        ResponseEntity<JsonNode> resp = rest.exchange(base() + "/admin/sources",
                HttpMethod.POST, new HttpEntity<>(body, adminHeaders()), JsonNode.class);
        ensure2xx(resp, "registerSource");
        return resp.getBody();
    }

    /** 注册基础结构：id(string,必填) + name(string,必填) + age(long,可选,默认0)。 */
    protected JsonNode registerBasicSchema(String sourceId) {
        ObjectNode schema = schema("id", List.of(
                Map.of("name", "id", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "name", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "age", "type", "LONG", "required", false, "defaultValue", "0")
        ));
        return postSchema("/admin/sources/" + sourceId + "/schema", schema, "registerSchema");
    }

    protected ObjectNode schema(String keyField, List<Map<String, Object>> columns) {
        ObjectNode schema = JsonFiles.mapper().createObjectNode();
        schema.put("keyField", keyField);
        ArrayNode arr = schema.putArray("columns");
        for (Map<String, Object> c : columns) {
            ObjectNode col = arr.addObject();
            col.put("name", (String) c.get("name"));
            col.put("type", (String) c.get("type"));
            col.put("required", (Boolean) c.get("required"));
            if (c.containsKey("defaultValue")) {
                col.put("defaultValue", (String) c.get("defaultValue"));
            }
        }
        return schema;
    }

    protected JsonNode evolveSchema(String sourceId, ObjectNode schema) {
        return postSchema("/admin/sources/" + sourceId + "/schema/evolve", schema, "evolveSchema");
    }

    private JsonNode postSchema(String path, ObjectNode schema, String op) {
        log.info("[SETUP] {}: {}", op, schema);
        ResponseEntity<JsonNode> resp = rest.exchange(base() + path,
                HttpMethod.POST, new HttpEntity<>(schema, adminHeaders()), JsonNode.class);
        ensure2xx(resp, op);
        return resp.getBody();
    }

    /** 构造一批记录请求：pairs 为 (offset, payloadObjectNode)。 */
    protected ObjectNode batch(List<Map.Entry<Long, ObjectNode>> pairs) {
        ObjectNode req = JsonFiles.mapper().createObjectNode();
        ArrayNode arr = req.putArray("records");
        for (Map.Entry<Long, ObjectNode> p : pairs) {
            ObjectNode item = arr.addObject();
            item.put("offset", p.getKey());
            item.set("payload", p.getValue());
        }
        return req;
    }

    protected ObjectNode record(String id, String name, long age) {
        ObjectNode n = JsonFiles.mapper().createObjectNode();
        n.put("id", id);
        n.put("name", name);
        n.put("age", age);
        return n;
    }

    protected ResponseEntity<JsonNode> push(String sourceId, String token, ObjectNode body) {
        return rest.exchange(base() + "/api/sources/" + sourceId + "/records",
                HttpMethod.POST, new HttpEntity<>(body, sourceHeaders(token)), JsonNode.class);
    }

    protected JsonNode getAdmin(String path) {
        ResponseEntity<JsonNode> resp = rest.exchange(base() + path,
                HttpMethod.GET, new HttpEntity<>(adminHeaders()), JsonNode.class);
        ensure2xx(resp, "GET " + path);
        return resp.getBody();
    }

    protected JsonNode postAdmin(String path) {
        ResponseEntity<JsonNode> resp = rest.exchange(base() + path,
                HttpMethod.POST, new HttpEntity<>(adminHeaders()), JsonNode.class);
        ensure2xx(resp, "POST " + path);
        return resp.getBody();
    }

    protected void ensure2xx(ResponseEntity<JsonNode> resp, String what) {
        if (!resp.getStatusCode().is2xxSuccessful()) {
            throw new AssertionError(what + " failed: " + resp.getStatusCode() + " " + resp.getBody());
        }
    }

    protected static Map.Entry<Long, ObjectNode> item(long offset, ObjectNode payload) {
        return Map.entry(offset, payload);
    }
}
