package com.example.httpparquet;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class JsonlWriterService {

    private static final Logger log = LoggerFactory.getLogger(JsonlWriterService.class);
    private static final DateTimeFormatter FILE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS").withZone(ZoneOffset.UTC);

    private final IngestionQueue ingestionQueue;
    private final Path outputDirectory;
    private final Clock clock;
    private final long maxIntervalMs;
    private final long maxBytes;

    private volatile Thread writerThread;

    public JsonlWriterService(
            IngestionQueue ingestionQueue,
            Path outputDirectory,
            Clock clock,
            @Value("${ingestion.flush.max-interval-ms}") long maxIntervalMs,
            @Value("${ingestion.flush.max-bytes}") long maxBytes) {
        this.ingestionQueue = ingestionQueue;
        this.outputDirectory = outputDirectory;
        this.clock = clock;
        this.maxIntervalMs = maxIntervalMs;
        this.maxBytes = maxBytes;
    }

    private static class WriterState {
        Instant currentHour;
        Path currentTmpPath;
        BufferedWriter currentWriter;
        long lastFlushMs;
        long unflushedBytes;

        WriterState(Instant hour, Path tmpPath, BufferedWriter writer, long nowMs) {
            this.currentHour = hour;
            this.currentTmpPath = tmpPath;
            this.currentWriter = writer;
            this.lastFlushMs = nowMs;
            this.unflushedBytes = 0;
        }
    }

    @PostConstruct
    public void start() {
        writerThread = Thread.ofVirtual().name("jsonl-writer").start(this::runLoop);
    }

    private void runLoop() {
        Map<String, WriterState> states = new HashMap<>();

        try {
            while (!Thread.currentThread().isInterrupted()) {
                TenantBatch batch = ingestionQueue.poll(100, TimeUnit.MILLISECONDS);

                Instant now = clock.instant();
                Instant hour = now.truncatedTo(ChronoUnit.HOURS);
                long nowMs = System.currentTimeMillis();

                if (batch != null) {
                    WriterState state = states.get(batch.tenantId());

                    if (state == null || !hour.equals(state.currentHour)) {
                        if (state != null) {
                            closeAndFinalize(state.currentWriter, state.currentTmpPath);
                        }
                        Path tmpPath = tmpPathFor(batch.tenantId(), now);
                        state = new WriterState(hour, tmpPath, openWriter(tmpPath), nowMs);
                        states.put(batch.tenantId(), state);
                    }

                    for (String record : batch.records()) {
                        state.currentWriter.write(record);
                        state.currentWriter.newLine();
                        state.unflushedBytes += record.getBytes(StandardCharsets.UTF_8).length + 1;
                    }
                }

                for (WriterState state : states.values()) {
                    if (state.unflushedBytes >= maxBytes ||
                            (state.unflushedBytes > 0 && nowMs - state.lastFlushMs >= maxIntervalMs)) {
                        state.currentWriter.flush();
                        state.lastFlushMs = nowMs;
                        state.unflushedBytes = 0;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.error("JSONL writer error, data may be lost", e);
        } finally {
            for (WriterState state : states.values()) {
                closeAndFinalize(state.currentWriter, state.currentTmpPath);
            }
        }
    }

    private BufferedWriter openWriter(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private void closeAndFinalize(BufferedWriter writer, Path tmpPath) {
        if (writer == null) return;
        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            log.warn("Failed to close writer for {}", tmpPath, e);
        }
        if (tmpPath == null) return;

        try {
            if (Files.size(tmpPath) == 0) {
                Files.deleteIfExists(tmpPath);
                return;
            }
        } catch (IOException e) {
            log.warn("Could not check size of {}", tmpPath, e);
        }

        Path jsonlPath = toJsonlPath(tmpPath);
        if (Files.exists(jsonlPath)) {
            // Collision: ms-precision timestamp makes this extremely unlikely. Merge to be safe.
            try (var in = Files.newInputStream(tmpPath);
                 var out = Files.newOutputStream(jsonlPath, StandardOpenOption.APPEND)) {
                in.transferTo(out);
            } catch (IOException e) {
                log.error("Failed to merge {} into existing {}: segment may be lost", tmpPath, jsonlPath, e);
                return;
            }
            try {
                Files.delete(tmpPath);
            } catch (IOException e) {
                log.warn("Failed to delete .tmp after merge: {}", tmpPath, e);
            }
        } else {
            try {
                Files.move(tmpPath, jsonlPath, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                log.warn("Failed to rename {} to {}: segment is stranded as .tmp and cannot be auto-recovered",
                        tmpPath, jsonlPath, e);
            }
        }
    }

    private Path tmpPathFor(String tenantId, Instant instant) throws IOException {
        Path tenantDir = outputDirectory.resolve(tenantId);
        Files.createDirectories(tenantDir);
        return tenantDir.resolve(FILE_FORMATTER.format(instant) + ".jsonl.tmp");
    }

    private Path toJsonlPath(Path tmpPath) {
        String name = tmpPath.getFileName().toString().replace(".jsonl.tmp", ".jsonl");
        return tmpPath.resolveSibling(name);
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (writerThread != null) {
            writerThread.interrupt();
            writerThread.join(10_000);
        }
    }
}
