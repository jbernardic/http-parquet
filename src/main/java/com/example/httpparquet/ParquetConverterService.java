package com.example.httpparquet;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

@Service("parquetConverterService")
public class ParquetConverterService {

    private static final Logger log = LoggerFactory.getLogger(ParquetConverterService.class);
    private static final Path POISON_PILL = Path.of("__SHUTDOWN__");

    private final Path outputDirectory;
    private final LinkedBlockingQueue<Path> queue = new LinkedBlockingQueue<>();

    private WatchService watchService;
    private volatile Thread watcherThread;
    private volatile Thread converterThread;

    public ParquetConverterService(Path outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    @PostConstruct
    public void start() throws IOException {
        watchService = FileSystems.getDefault().newWatchService();

        // Watch root for new tenant directory creation
        outputDirectory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        // Register watchers and scan all existing tenant directories
        try (Stream<Path> subdirs = Files.list(outputDirectory)) {
            subdirs.filter(Files::isDirectory).forEach(tenantDir -> {
                try {
                    registerAndScanTenantDir(tenantDir);
                } catch (IOException e) {
                    log.warn("Could not register watcher for tenant dir: {}", tenantDir.getFileName(), e);
                }
            });
        }

        watcherThread = Thread.ofVirtual().name("jsonl-watcher").start(this::watchLoop);
        converterThread = Thread.ofVirtual().name("parquet-converter").start(this::convertLoop);
    }

    private void registerAndScanTenantDir(Path tenantDir) throws IOException {
        tenantDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        try (Stream<Path> stream = Files.list(tenantDir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".parquet.tmp"))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                            log.warn("Deleted orphaned .parquet.tmp: {}", p);
                        } catch (IOException e) {
                            log.warn("Could not delete orphaned .parquet.tmp: {}", p, e);
                        }
                    });
        }

        try (Stream<Path> stream = Files.list(tenantDir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".jsonl"))
                    .filter(p -> !Files.exists(toParquetPath(p)))
                    .filter(this::isNonEmpty)
                    .sorted(Comparator.naturalOrder())
                    .forEach(p -> {
                        try {
                            queue.put(p);
                            log.info("Queued existing .jsonl for conversion: {}", p);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
        }
    }

    private void watchLoop() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key;
                try {
                    key = watchService.take();
                } catch (ClosedWatchServiceException e) {
                    break;
                }
                Path watchedDir = (Path) key.watchable();

                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) continue;
                    Path name = (Path) event.context();
                    Path fullPath = watchedDir.resolve(name);

                    if (watchedDir.equals(outputDirectory)) {
                        if (Files.isDirectory(fullPath)) {
                            try {
                                registerAndScanTenantDir(fullPath);
                            } catch (IOException e) {
                                log.warn("Could not register new tenant dir: {}", fullPath.getFileName(), e);
                            }
                        }
                    } else {
                        if (name.toString().endsWith(".jsonl") && isNonEmpty(fullPath)) {
                            queue.put(fullPath);
                        }
                    }
                }
                key.reset();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void convertLoop() {
        try {
            while (true) {
                Path path = queue.take();
                if (POISON_PILL.equals(path)) break;
                convert(path);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void convert(Path jsonlFile) {
        Path parquetFile = toParquetPath(jsonlFile);
        if (Files.exists(parquetFile)) {
            log.info("Parquet already exists, skipping: {}", parquetFile);
            return;
        }

        Path parquetTmpFile = parquetFile.resolveSibling(parquetFile.getFileName() + ".tmp");
        String jsonlPath = jsonlFile.toAbsolutePath().toString().replace('\\', '/');
        String parquetTmpPath = parquetTmpFile.toAbsolutePath().toString().replace('\\', '/');
        String sql = String.format(
                "COPY (SELECT * FROM read_ndjson_auto('%s')) TO '%s' (FORMAT PARQUET)",
                jsonlPath.replace("'", "''"), parquetTmpPath.replace("'", "''"));

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            log.error("DuckDB conversion failed for {}, will retry on next startup", jsonlFile, e);
            try { Files.deleteIfExists(parquetTmpFile); } catch (IOException ignored) {}
            return;
        }

        try {
            Files.move(parquetTmpFile, parquetFile, StandardCopyOption.ATOMIC_MOVE);
            log.info("Converted {} -> {}", jsonlFile.getFileName(), parquetFile.getFileName());
        } catch (IOException e) {
            log.error("Failed to rename {} to final .parquet", parquetTmpFile.getFileName(), e);
            try { Files.deleteIfExists(parquetTmpFile); } catch (IOException ignored) {}
            return;
        }

        try {
            Files.deleteIfExists(jsonlFile);
        } catch (IOException e) {
            log.warn("Conversion succeeded but failed to delete {}", jsonlFile);
        }
    }

    private Path toParquetPath(Path jsonlFile) {
        String name = jsonlFile.getFileName().toString().replace(".jsonl", ".parquet");
        return jsonlFile.resolveSibling(name);
    }

    private boolean isNonEmpty(Path file) {
        try {
            return Files.size(file) > 0;
        } catch (IOException e) {
            return false;
        }
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (watchService != null) {
            try { watchService.close(); } catch (IOException e) { log.warn("Error closing watcher", e); }
        }
        if (watcherThread != null) watcherThread.join(5_000);
        if (converterThread != null) {
            queue.put(POISON_PILL);
            converterThread.join(60_000);
        }
    }
}
