package com.github.highcumontoa.ingestionpipelinejava.config;

import com.github.highcumontoa.ingestionpipelinejava.core.AuthService;
import com.github.highcumontoa.ingestionpipelinejava.core.BacklogGuard;
import com.github.highcumontoa.ingestionpipelinejava.core.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.store.DataStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import com.github.highcumontoa.ingestionpipelinejava.store.QuarantineStore;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 管线各组件的装配。全部持久化组件共享同一数据根目录，保证重启恢复。
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineBeans {

    @Bean
    public Path dataDir(PipelineProperties props) {
        Path p = Path.of(props.getDataDir()).toAbsolutePath();
        java.io.File dir = p.toFile();
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("cannot create data dir: " + p);
        }
        return p;
    }

    @Bean
    public OffsetStore offsetStore(Path dataDir) {
        return new OffsetStore(dataDir);
    }

    @Bean
    public DataStore dataStore(Path dataDir) {
        return new DataStore(dataDir);
    }

    @Bean
    public SchemaStore schemaStore(Path dataDir) {
        return new SchemaStore(dataDir);
    }

    @Bean
    public QuarantineStore quarantineStore(Path dataDir) {
        return new QuarantineStore(dataDir);
    }

    @Bean
    public MetricsStore metricsStore(Path dataDir) {
        return new MetricsStore(dataDir);
    }

    @Bean
    public BacklogGuard backlogGuard(PipelineProperties props) {
        return new BacklogGuard(props);
    }

    @Bean
    public SourceRegistry sourceRegistry(PipelineProperties props, Path dataDir) {
        return new SourceRegistry(props, dataDir);
    }

    @Bean
    public AuthService authService(SourceRegistry registry, PipelineProperties props) {
        return new AuthService(registry, props);
    }

    @Bean
    public IngestionService ingestionService(OffsetStore offsets, DataStore data, SchemaStore schemas,
                                            QuarantineStore quarantine, MetricsStore metrics,
                                            BacklogGuard backlog, PipelineProperties props) {
        return new IngestionService(offsets, data, schemas, quarantine, metrics, backlog, props);
    }
}
