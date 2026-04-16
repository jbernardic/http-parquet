package com.example.httpparquet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

class JsonlWriterServiceTest {

    @TempDir
    Path tempDir;

    private IngestionQueue ingestionQueue;
    private AtomicReference<Instant> clockInstant;
    private JsonlWriterService service;

    @BeforeEach
    void setUp() {
        ingestionQueue = new IngestionQueue();
        clockInstant = new AtomicReference<>(Instant.parse("2026-04-16T14:00:00Z"));
    }

    private JsonlWriterService createService(long maxIntervalMs, long maxBytes) {
        Clock mutableClock = new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { return clockInstant.get(); }
        };
        return new JsonlWriterService(ingestionQueue, tempDir, mutableClock, maxIntervalMs, maxBytes);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (service != null) service.stop();
    }

    // -- Helpers -------------------------------------------------------------

    private Optional<Path> findFirst(String suffix) throws IOException {
        try (var stream = Files.walk(tempDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(suffix)).findFirst();
        }
    }

    private long countFiles(String suffix) throws IOException {
        try (var stream = Files.walk(tempDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(suffix)).count();
        }
    }

    // -- Tests ---------------------------------------------------------------

    @Test
    void writesRecordToTmpFile() throws Exception {
        service = createService(100, 65536);
        service.start();

        ingestionQueue.put(new TenantBatch("tenant1", List.of("{\"event\":\"test\"}")));

        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.readString(tmp.get()).contains("\"event\":\"test\"");
            } catch (IOException e) { return false; }
        });
    }

    @Test
    void tmpFileNameHasFullTimestampWithMilliseconds() throws Exception {
        service = createService(100, 65536);
        service.start();

        ingestionQueue.put(new TenantBatch("tenant1", List.of("{\"a\":1}")));

        await().atMost(5, SECONDS).until(() -> {
            try { return findFirst(".jsonl.tmp").isPresent(); }
            catch (IOException e) { return false; }
        });

        Path tmp = findFirst(".jsonl.tmp").orElseThrow();
        assertThat(tmp.getFileName().toString())
                .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}\\.jsonl\\.tmp");
    }

    @Test
    void rollsFileOnHourChange() throws Exception {
        service = createService(100, 65536);
        service.start();

        ingestionQueue.put(new TenantBatch("tenant1", List.of("{\"hour\":\"14\"}")));

        // Wait for data to be flushed to the .tmp file
        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.size(tmp.get()) > 0;
            } catch (IOException e) { return false; }
        });

        // Advance to next hour and send a record to trigger the rollover
        clockInstant.set(Instant.parse("2026-04-16T15:00:00Z"));
        ingestionQueue.put(new TenantBatch("tenant1", List.of("{\"hour\":\"15\"}")));

        // Completed .jsonl should appear — the converter (separate service) picks it up from here
        await().atMost(5, SECONDS).until(() -> {
            try { return findFirst(".jsonl").isPresent(); }
            catch (IOException e) { return false; }
        });

        // A new .tmp for the next hour should be created
        await().atMost(5, SECONDS).until(() -> {
            try { return countFiles(".jsonl.tmp") == 1; }
            catch (IOException e) { return false; }
        });

        Path jsonl = findFirst(".jsonl").orElseThrow();
        assertThat(jsonl.getFileName().toString())
                .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}\\.jsonl");
    }

    @Test
    void shutdownRenamesTmpToJsonl() throws Exception {
        service = createService(100, 65536);
        service.start();

        ingestionQueue.put(new TenantBatch("tenant1", List.of("{\"event\":\"shutdown-test\"}")));

        // Wait for data to land in the .tmp file
        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.size(tmp.get()) > 0;
            } catch (IOException e) { return false; }
        });

        service.stop();
        service = null;

        // .tmp replaced by .jsonl — converter picks it up on next startup
        assertThat(findFirst(".jsonl.tmp")).isEmpty();
        Path jsonl = findFirst(".jsonl").orElseThrow();
        assertThat(Files.size(jsonl)).isGreaterThan(0);
        assertThat(Files.readString(jsonl)).contains("shutdown-test");
    }

    @Test
    void emptyFileIsDeletedNotRenamedOnRoll() throws Exception {
        service = createService(60_000, 65536);
        service.start();

        // An empty batch opens a 0-byte .tmp file without writing any records
        ingestionQueue.put(new TenantBatch("tenant1", List.of()));

        // Wait for the empty .tmp to be created
        await().atMost(5, SECONDS).until(() -> {
            try { return findFirst(".jsonl.tmp").isPresent(); }
            catch (IOException e) { return false; }
        });

        // Advance to next hour and send a record to trigger the rollover
        clockInstant.set(Instant.parse("2026-04-16T15:00:00Z"));
        ingestionQueue.put(new TenantBatch("tenant1", List.of("{\"x\":1}")));

        // Wait for the new hour's .tmp to appear
        await().atMost(5, SECONDS).until(() -> {
            try {
                return findFirst(".jsonl.tmp")
                        .map(p -> p.getFileName().toString().startsWith("2026-04-16T15"))
                        .orElse(false);
            } catch (IOException e) { return false; }
        });

        // Empty file must have been deleted, not promoted to .jsonl
        assertThat(findFirst(".jsonl")).isEmpty();
    }

    @Test
    void flushesAfterMaxInterval() throws Exception {
        // Uses real wall-clock time (flush interval is not controlled by the injected clock)
        service = createService(200, 65536);
        service.start();

        ingestionQueue.put(new TenantBatch("tenant1", List.of("{\"a\":1}")));

        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.readString(tmp.get()).contains("\"a\":1");
            } catch (IOException e) { return false; }
        });
    }

    @Test
    void flushesAfterMaxBytes() throws Exception {
        service = createService(60_000, 1);
        service.start();

        ingestionQueue.put(new TenantBatch("tenant1", List.of("{\"x\":1}")));

        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.readString(tmp.get()).contains("\"x\":1");
            } catch (IOException e) { return false; }
        });
    }
}
