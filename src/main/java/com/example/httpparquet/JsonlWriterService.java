package com.example.httpparquet;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@DependsOn("parquetConverterService")
public class JsonlWriterService {

    private static final Logger log = LoggerFactory.getLogger(JsonlWriterService.class);
    private static final DateTimeFormatter FILE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS").withZone(ZoneOffset.UTC);

    private final IngestionQueue ingestionQueue;
    private final ConversionQueue conversionQueue;
    private final Path outputDirectory;
    private final Clock clock;
    private final long maxIntervalMs;
    private final long maxBytes;

    private volatile Thread writerThread;

    public JsonlWriterService(
            IngestionQueue ingestionQueue,
            ConversionQueue conversionQueue,
            Path outputDirectory,
            Clock clock,
            @Value("${ingestion.flush.max-interval-ms}") long maxIntervalMs,
            @Value("${ingestion.flush.max-bytes}") long maxBytes) {
        this.ingestionQueue = ingestionQueue;
        this.conversionQueue = conversionQueue;
        this.outputDirectory = outputDirectory;
        this.clock = clock;
        this.maxIntervalMs = maxIntervalMs;
        this.maxBytes = maxBytes;
    }

    @PostConstruct
    public void start() {
        writerThread = Thread.ofVirtual().name("jsonl-writer").start(this::runLoop);
    }

    private void runLoop() {
        Instant currentHour = null;
        Path currentTmpPath = null;
        BufferedWriter currentWriter = null;
        long lastFlushMs = System.currentTimeMillis();
        long unflushedBytes = 0;

        try {
            while (!Thread.currentThread().isInterrupted()) {
                List<String> batch = ingestionQueue.poll(100, TimeUnit.MILLISECONDS);

                Instant now = clock.instant();
                Instant hour = now.truncatedTo(ChronoUnit.HOURS);
                if (!hour.equals(currentHour)) {
                    BufferedWriter prev = currentWriter;
                    Path prevTmpPath = currentTmpPath;
                    currentWriter = null;  // clear before closeAndFinalize so finally block skips stale ref on IOException
                    currentTmpPath = null;
                    closeAndFinalize(prev, prevTmpPath, true);  // normal hourly roll — enqueue for conversion
                    currentTmpPath = tmpPathFor(now);
                    currentWriter = openWriter(currentTmpPath);
                    currentHour = hour;
                    lastFlushMs = System.currentTimeMillis();
                    unflushedBytes = 0;
                }

                if (batch != null) {
                    for (String record : batch) {
                        currentWriter.write(record);
                        currentWriter.newLine();
                        unflushedBytes += record.getBytes(StandardCharsets.UTF_8).length + 1;
                    }
                }

                long nowMs = System.currentTimeMillis();
                if (unflushedBytes >= maxBytes || (unflushedBytes > 0 && nowMs - lastFlushMs >= maxIntervalMs)) {
                    currentWriter.flush();
                    lastFlushMs = nowMs;
                    unflushedBytes = 0;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.error("JSONL writer error, data may be lost", e);
        } finally {
            closeAndFinalize(currentWriter, currentTmpPath, false);  // shutdown — rename only, no conversion
        }
    }

    private BufferedWriter openWriter(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * Flushes, closes, and renames the current .tmp file to .jsonl.
     * If {@code enqueue} is true (normal hourly roll), the .jsonl is put on the conversion
     * queue immediately. If false (shutdown), it is left on disk for the converter to pick
     * up on next startup via {@code recoverOrphanedFiles}.
     */
    private void closeAndFinalize(BufferedWriter writer, Path tmpPath, boolean enqueue) {
        if (writer == null) return;
        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            log.warn("Failed to close writer for {}", tmpPath, e);
        }
        if (tmpPath == null) return;

        Path jsonlPath = toJsonlPath(tmpPath);
        if (Files.exists(jsonlPath)) {
            // Collision: ms-precision timestamps make this extremely unlikely, but handle it
            // safely by streaming the .tmp content onto the end of the existing .jsonl.
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
                return;
            }
        }

        if (!enqueue) {
            log.info("Shutdown: {} will be converted on next startup", jsonlPath.getFileName());
            return;
        }

        try {
            if (Files.size(jsonlPath) > 0) {
                // Temporarily clear the interrupt flag: lockInterruptibly() inside put() would
                // fire immediately on an interrupted thread even on an unbounded queue.
                boolean wasInterrupted = Thread.interrupted();
                try {
                    conversionQueue.put(jsonlPath);
                } catch (InterruptedException e) {
                    wasInterrupted = true;
                    log.warn("Interrupted while queuing {}; file will be recovered on next startup", jsonlPath);
                } finally {
                    if (wasInterrupted) Thread.currentThread().interrupt();
                }
            } else {
                Files.deleteIfExists(jsonlPath);
            }
        } catch (IOException e) {
            log.warn("Could not check size of {}", jsonlPath, e);
        }
    }

    private Path tmpPathFor(Instant instant) {
        return outputDirectory.resolve(FILE_FORMATTER.format(instant) + ".jsonl.tmp");
    }

    private Path toJsonlPath(Path tmpPath) {
        String name = tmpPath.getFileName().toString().replace(".jsonl.tmp", ".jsonl");
        return tmpPath.getParent().resolve(name);
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (writerThread != null) {
            writerThread.interrupt();
            writerThread.join(10_000);
        }
    }
}
