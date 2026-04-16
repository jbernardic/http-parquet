package com.example.httpparquet;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.stream.Stream;

@Service("parquetConverterService")
public class ParquetConverterService {

    private static final Logger log = LoggerFactory.getLogger(ParquetConverterService.class);
    private static final Path POISON_PILL = Path.of("__SHUTDOWN__");

    private final ConversionQueue conversionQueue;
    private final Path outputDirectory;

    private volatile Thread converterThread;

    public ParquetConverterService(ConversionQueue conversionQueue, Path outputDirectory) {
        this.conversionQueue = conversionQueue;
        this.outputDirectory = outputDirectory;
    }

    @PostConstruct
    public void start() throws IOException {
        recoverOrphanedFiles();
        converterThread = Thread.ofVirtual().name("parquet-converter").start(this::runLoop);
    }

    private void recoverOrphanedFiles() throws IOException {
        try (Stream<Path> stream = Files.list(outputDirectory)) {
            stream
                    .filter(p -> p.getFileName().toString().endsWith(".jsonl"))
                    .filter(p -> !Files.exists(toParquetPath(p)))
                    .filter(this::isNonEmpty)
                    .sorted(Comparator.naturalOrder())
                    .forEach(p -> {
                        try {
                            conversionQueue.put(p);
                            log.info("Queued orphaned file for conversion: {}", p.getFileName());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
        }
    }

    private void runLoop() {
        try {
            while (true) {
                Path path = conversionQueue.take();
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
            log.info("Parquet already exists, skipping: {}", parquetFile.getFileName());
            return;
        }

        String jsonlPath = jsonlFile.toAbsolutePath().toString().replace('\\', '/');
        String parquetPath = parquetFile.toAbsolutePath().toString().replace('\\', '/');
        String sql = String.format(
                "COPY (SELECT * FROM read_ndjson_auto('%s')) TO '%s' (FORMAT PARQUET)",
                jsonlPath.replace("'", "''"), parquetPath.replace("'", "''"));

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            stmt.execute(sql);
            log.info("Converted {} -> {}", jsonlFile.getFileName(), parquetFile.getFileName());

        } catch (SQLException e) {
            log.error("DuckDB conversion failed for {}, will retry on next startup",
                    jsonlFile.getFileName(), e);
            return;
        }

        try {
            Files.deleteIfExists(jsonlFile);
        } catch (IOException e) {
            log.warn("Conversion succeeded but failed to delete {}, leaving file for manual cleanup",
                    jsonlFile.getFileName(), e);
        }
    }

    private Path toParquetPath(Path jsonlFile) {
        String name = jsonlFile.getFileName().toString().replace(".jsonl", ".parquet");
        return outputDirectory.resolve(name);
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
        if (converterThread != null) {
            conversionQueue.put(POISON_PILL);
            converterThread.join(60_000);
        }
    }
}
