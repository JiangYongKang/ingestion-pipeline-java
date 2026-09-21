package com.github.highcumontoa.ingestionpipelinejava.web;

import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.service.AuthService;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceDefinition;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 本地 HTTP 拉取源模拟器。
 * <ul>
 *   <li>POST /mock/sources/{source}/seed（需该来源凭据）：追加模拟记录；</li>
 *   <li>GET  /mock/sources/{source}/records?after&limit（需凭据）：
 *       返回从 after+1 开始的连续序号记录，供 HttpSourceReader 拉取。</li>
 * </ul>
 */
@RestController
@RequestMapping("/mock/sources/{source}")
public class MockSourceController {

    private final AuthService auth;
    private final SourceRegistry registry;
    private final ConcurrentMap<String, List<IncomingRecord>> store = new ConcurrentHashMap<>();

    public MockSourceController(AuthService auth, SourceRegistry registry) {
        this.auth = auth;
        this.registry = registry;
    }

    @PostMapping("/seed")
    public Map<String, Object> seed(@PathVariable String source,
                                    @RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                    @RequestBody List<IncomingRecord> records) {
        SourceDefinition def = auth.requireAccess(source, apiKey);
        List<IncomingRecord> list = store.computeIfAbsent(def.name(), k -> new ArrayList<>());
        synchronized (list) {
            list.addAll(records);
            return Map.of("total", list.size(), "appended", records.size());
        }
    }

    @GetMapping("/records")
    public Map<String, Object> records(@PathVariable String source,
                                       @RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                       @RequestParam(defaultValue = "0") long after,
                                       @RequestParam(defaultValue = "100") int limit) {
        SourceDefinition def = auth.requireAccess(source, apiKey);
        List<IncomingRecord> list = store.getOrDefault(def.name(), List.of());
        List<Map<String, Object>> out = new ArrayList<>();
        synchronized (list) {
            for (int index = (int) after; index < list.size() && out.size() < limit; index++) {
                IncomingRecord rec = list.get(index);
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("sequence", index + 1); // 位点从 1 起，连续
                item.put("key", rec.key());
                item.put("data", rec.data());
                out.add(item);
            }
        }
        return Map.of("records", out);
    }
}
