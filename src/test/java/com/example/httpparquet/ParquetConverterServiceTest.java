package com.example.httpparquet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

class ParquetConverterServiceTest {

    @TempDir
    Path tempDir;

    private ParquetConverterService service;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (service != null) service.stop();
    }

    private void createAndStart() throws IOException {
        service = new ParquetConverterService(tempDir);
        service.start();
    }

    @Test
    void convertsJsonlToParquet() throws Exception {
        Path jsonl = tempDir.resolve("2026-04-16T14.jsonl");
        Files.writeString(jsonl, "{\"event\":\"click\",\"value\":42}\n{\"event\":\"view\",\"value\":1}\n");

        createAndStart();  // startup scan picks up the pre-existing .jsonl

        Path parquet = tempDir.resolve("2026-04-16T14.parquet");
        await().atMost(10, SECONDS).until(() -> Files.exists(parquet));
        assertThat(Files.size(parquet)).isGreaterThan(0);

        // Source .jsonl must be deleted after successful conversion
        assertThat(Files.exists(jsonl)).isFalse();

        // Verify the parquet is readable and has the right row count
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
    void picksUpNewJsonlViaWatcher() throws Exception {
        createAndStart();  // no files yet — exercises the watcher path, not the startup scan

        // Drop a .jsonl after the service is already running.
        // Write to a .tmp first, then rename — this matches what JsonlWriterService does in
        // production and guarantees the ENTRY_CREATE event fires only after the file is fully
        // written (on Windows, ReadDirectoryChangesW can fire ENTRY_CREATE before content is
        // flushed if the file is created and written in a single open/write/close sequence).
        Path tmp = tempDir.resolve("2026-04-16T15.jsonl.tmp");
        Files.writeString(tmp, "{\"via\":\"watcher\"}\n");
        Path jsonl = tempDir.resolve("2026-04-16T15.jsonl");
        Files.move(tmp, jsonl);

        Path parquet = tempDir.resolve("2026-04-16T15.parquet");
        await().atMost(10, SECONDS).until(() -> Files.exists(parquet));
        assertThat(Files.size(parquet)).isGreaterThan(0);
        assertThat(Files.exists(jsonl)).isFalse();
    }

    @Test
    void skipsAlreadyConvertedFile() throws Exception {
        Path jsonl = tempDir.resolve("2026-04-16T14.jsonl");
        Files.writeString(jsonl, "{\"a\":1}\n");

        Path parquet = tempDir.resolve("2026-04-16T14.parquet");
        Files.writeString(parquet, "existing");  // pre-existing parquet (fake)

        createAndStart();
        Thread.sleep(500);

        // Fake parquet must not have been overwritten
        assertThat(Files.readString(parquet)).isEqualTo("existing");
    }

    @Test
    void recoversOrphanedFilesOnStartup() throws Exception {
        Path jsonl = tempDir.resolve("2026-04-16T13.jsonl");
        Files.writeString(jsonl, "{\"orphaned\":true}\n");

        createAndStart();

        Path parquet = tempDir.resolve("2026-04-16T13.parquet");
        await().atMost(10, SECONDS).until(() -> Files.exists(parquet));
        assertThat(Files.size(parquet)).isGreaterThan(0);
        assertThat(Files.exists(jsonl)).isFalse();
    }

    @Test
    void ignoresTmpFilesOnRecovery() throws Exception {
        Path tmp = tempDir.resolve("2026-04-16T14.jsonl.tmp");
        Files.writeString(tmp, "{\"in-progress\":true}\n");

        createAndStart();
        Thread.sleep(500);

        assertThat(Files.exists(tempDir.resolve("2026-04-16T14.parquet"))).isFalse();
    }

    @Test
    void deletesOrphanedParquetTmpOnStartup() throws Exception {
        Path parquetTmp = tempDir.resolve("2026-04-16T14.parquet.tmp");
        Files.writeString(parquetTmp, "incomplete");

        createAndStart();
        Thread.sleep(500);

        // Orphaned .parquet.tmp must be cleaned up
        assertThat(Files.exists(parquetTmp)).isFalse();
    }
}
