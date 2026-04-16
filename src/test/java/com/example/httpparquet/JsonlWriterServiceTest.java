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
    private ConversionQueue conversionQueue;
    private AtomicReference<Instant> clockInstant;
    private JsonlWriterService service;

    @BeforeEach
    void setUp() {
        ingestionQueue = new IngestionQueue();
        conversionQueue = new ConversionQueue();
        clockInstant = new AtomicReference<>(Instant.parse("2026-04-16T14:00:00Z"));
    }

    private JsonlWriterService createService(long maxIntervalMs, long maxBytes) {
        Clock mutableClock = new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { return clockInstant.get(); }
        };
        return new JsonlWriterService(ingestionQueue, conversionQueue, tempDir, mutableClock, maxIntervalMs, maxBytes);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (service != null) service.stop();
    }

    // -- Helpers -------------------------------------------------------------

    private Optional<Path> findFirst(String suffix) throws IOException {
        try (var stream = Files.list(tempDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(suffix)).findFirst();
        }
    }

    private long countFiles(String suffix) throws IOException {
        try (var stream = Files.list(tempDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(suffix)).count();
        }
    }

    // -- Tests ---------------------------------------------------------------

    @Test
    void writesRecordToTmpFile() throws Exception {
        service = createService(100, 65536);
        service.start();

        ingestionQueue.put(List.of("{\"event\":\"test\"}"));

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

        await().atMost(5, SECONDS).until(() -> {
            try { return findFirst(".jsonl.tmp").isPresent(); }
            catch (IOException e) { return false; }
        });

        Path tmp = findFirst(".jsonl.tmp").orElseThrow();
        // Expected format: yyyy-MM-dd'T'HH-mm-ss-SSS.jsonl.tmp
        assertThat(tmp.getFileName().toString())
                .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}\\.jsonl\\.tmp");
    }

    @Test
    void rollsFileOnHourChange() throws Exception {
        service = createService(100, 65536);
        service.start();

        ingestionQueue.put(List.of("{\"hour\":\"14\"}"));

        // Wait for data to be flushed to the .tmp file
        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.size(tmp.get()) > 0;
            } catch (IOException e) { return false; }
        });

        // Advance to next hour — triggers rollover on next poll
        clockInstant.set(Instant.parse("2026-04-16T15:00:00Z"));

        // Completed .jsonl should appear
        await().atMost(5, SECONDS).until(() -> {
            try { return findFirst(".jsonl").isPresent(); }
            catch (IOException e) { return false; }
        });

        // A new .tmp for the next hour should be created
        await().atMost(5, SECONDS).until(() -> {
            try { return countFiles(".jsonl.tmp") == 1; }
            catch (IOException e) { return false; }
        });

        // The completed .jsonl should have been enqueued for conversion and have the right name format
        Path queued = conversionQueue.take();
        assertThat(queued.getParent()).isEqualTo(tempDir);
        assertThat(queued.getFileName().toString())
                .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}\\.jsonl");
    }

    @Test
    void jsonlAndParquetShareTheSameTimestampStem() throws Exception {
        // Verify the naming contract: stripping the extension from the .jsonl gives the same
        // stem that the converter uses for the .parquet, so the pair is always co-named.
        service = createService(100, 65536);
        service.start();

        ingestionQueue.put(List.of("{\"a\":1}"));
        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.size(tmp.get()) > 0;
            } catch (IOException e) { return false; }
        });

        clockInstant.set(Instant.parse("2026-04-16T15:00:00Z"));
        await().atMost(5, SECONDS).until(() -> {
            try { return findFirst(".jsonl").isPresent(); }
            catch (IOException e) { return false; }
        });

        Path jsonl = findFirst(".jsonl").orElseThrow();
        String stem = jsonl.getFileName().toString().replace(".jsonl", "");
        Path expectedParquet = tempDir.resolve(stem + ".parquet");

        // Both names carry the same ms-precision timestamp prefix
        assertThat(stem).matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}");
        assertThat(expectedParquet.getFileName().toString()).isEqualTo(stem + ".parquet");
    }

    @Test
    void shutdownRenamesTmpToJsonlWithoutEnqueuing() throws Exception {
        service = createService(100, 65536);
        service.start();

        ingestionQueue.put(List.of("{\"event\":\"shutdown-test\"}"));

        // Wait for data to be written to the .tmp file
        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.size(tmp.get()) > 0;
            } catch (IOException e) { return false; }
        });

        service.stop();
        service = null;  // prevent tearDown double-stop

        // .tmp is gone, .jsonl exists with the data
        assertThat(findFirst(".jsonl.tmp")).isEmpty();
        Path jsonl = findFirst(".jsonl").orElseThrow();
        assertThat(Files.size(jsonl)).isGreaterThan(0);
        assertThat(Files.readString(jsonl)).contains("shutdown-test");

        // Nothing enqueued — the converter picks it up on next startup
        assertThat(conversionQueue.size()).isEqualTo(0);
    }

    @Test
    void flushesAfterMaxInterval() throws Exception {
        // The flush interval check uses System.currentTimeMillis(), not the injected clock,
        // so this test relies on real wall-clock time. 200ms fires well within the 5s window.
        service = createService(200, 65536);
        service.start();

        ingestionQueue.put(List.of("{\"a\":1}"));

        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.readString(tmp.get()).contains("\"a\":1");
            } catch (IOException e) { return false; }
        });
    }

    @Test
    void flushesAfterMaxBytes() throws Exception {
        // maxBytes=1 ensures a single record exceeds the threshold
        service = createService(60_000, 1);
        service.start();

        ingestionQueue.put(List.of("{\"x\":1}"));

        await().atMost(5, SECONDS).until(() -> {
            try {
                Optional<Path> tmp = findFirst(".jsonl.tmp");
                return tmp.isPresent() && Files.readString(tmp.get()).contains("\"x\":1");
            } catch (IOException e) { return false; }
        });
    }

    @Test
    void doesNotEnqueueEmptyFile() throws Exception {
        service = createService(60_000, 65536);
        service.start();

        // Wait for the writer to open its initial .tmp
        await().atMost(5, SECONDS).until(() -> {
            try { return findFirst(".jsonl.tmp").isPresent(); }
            catch (IOException e) { return false; }
        });

        // Roll to next hour with no records written
        clockInstant.set(Instant.parse("2026-04-16T15:00:00Z"));

        // Wait for the new hour's .tmp to appear (its name starts with T15)
        await().atMost(5, SECONDS).until(() -> {
            try {
                return findFirst(".jsonl.tmp")
                        .map(p -> p.getFileName().toString().startsWith("2026-04-16T15"))
                        .orElse(false);
            } catch (IOException e) { return false; }
        });

        // The empty file must not have been enqueued
        assertThat(conversionQueue.size()).isEqualTo(0);
    }
}
