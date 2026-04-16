package com.example.httpparquet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class ParquetConverterServiceTest {

    @TempDir
    Path tempDir;

    private ConversionQueue conversionQueue;
    private ParquetConverterService service;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (service != null) service.stop();
    }

    private void createAndStart() throws IOException {
        conversionQueue = new ConversionQueue();
        service = new ParquetConverterService(conversionQueue, tempDir, mock(GcsUploadService.class));
        service.start();
    }

    @Test
    void convertsJsonlToParquet() throws Exception {
        Path jsonl = tempDir.resolve("2026-04-16T14.jsonl");
        Files.writeString(jsonl, "{\"event\":\"click\",\"value\":42}\n{\"event\":\"view\",\"value\":1}\n");

        createAndStart();
        conversionQueue.put(jsonl);

        Path parquet = tempDir.resolve("2026-04-16T14.parquet");
        await().atMost(10, SECONDS).until(() -> Files.exists(parquet));
        assertThat(Files.size(parquet)).isGreaterThan(0);

        // Verify readable by DuckDB
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             var stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM read_parquet('" +
                     parquet.toAbsolutePath().toString().replace('\\', '/') + "')")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(2);
        }
    }

    @Test
    void skipsAlreadyConvertedFile() throws Exception {
        Path jsonl = tempDir.resolve("2026-04-16T14.jsonl");
        Files.writeString(jsonl, "{\"a\":1}\n");

        Path parquet = tempDir.resolve("2026-04-16T14.parquet");
        Files.writeString(parquet, "existing");  // pre-existing parquet (fake)

        createAndStart();
        conversionQueue.put(jsonl);

        // Give service time to process
        Thread.sleep(500);

        // File should not have been overwritten
        assertThat(Files.readString(parquet)).isEqualTo("existing");
    }

    @Test
    void recoversOrphanedFilesOnStartup() throws Exception {
        // Pre-create a .jsonl file with no .parquet counterpart
        Path jsonl = tempDir.resolve("2026-04-16T13.jsonl");
        Files.writeString(jsonl, "{\"orphaned\":true}\n");

        createAndStart();  // recoverOrphanedFiles runs in @PostConstruct

        Path parquet = tempDir.resolve("2026-04-16T13.parquet");
        await().atMost(10, SECONDS).until(() -> Files.exists(parquet));
        assertThat(Files.size(parquet)).isGreaterThan(0);
    }

    @Test
    void ignoresTmpFilesOnRecovery() throws Exception {
        // A .jsonl.tmp file should NOT be picked up during recovery
        Path tmp = tempDir.resolve("2026-04-16T14.jsonl.tmp");
        Files.writeString(tmp, "{\"in-progress\":true}\n");

        createAndStart();
        Thread.sleep(500);

        // No .parquet should have been created
        assertThat(Files.exists(tempDir.resolve("2026-04-16T14.parquet"))).isFalse();
    }
}
