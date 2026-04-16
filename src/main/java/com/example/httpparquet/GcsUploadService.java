package com.example.httpparquet;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

@Service("gcsUploadService")
public class GcsUploadService {

    private static final Logger log = LoggerFactory.getLogger(GcsUploadService.class);
    private static final Path POISON_PILL = Path.of("__SHUTDOWN__");

    private final Path outputDirectory;

    @Value("${gcs.enabled:false}")
    private boolean enabled;

    @Value("${gcs.bucket-name:}")
    private String bucketName;

    @Value("${gcs.credentials-path:}")
    private String credentialsPath;

    @Value("${gcs.object-prefix:}")
    private String objectPrefix;

    private final LinkedBlockingQueue<Path> queue = new LinkedBlockingQueue<>();
    private Storage storage;
    private WatchService watchService;
    private volatile Thread watcherThread;
    private volatile Thread uploaderThread;

    public GcsUploadService(Path outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    @PostConstruct
    public void start() throws IOException {
        if (!enabled) {
            log.info("GCS upload disabled (gcs.enabled=false)");
            return;
        }

        StorageOptions.Builder builder = StorageOptions.newBuilder();
        if (!credentialsPath.isBlank()) {
            try (FileInputStream stream = new FileInputStream(credentialsPath)) {
                builder.setCredentials(
                        GoogleCredentials.fromStream(stream)
                                .createScoped("https://www.googleapis.com/auth/cloud-platform"));
            }
        }
        storage = builder.build().getService();

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

        watcherThread = Thread.ofVirtual().name("parquet-watcher").start(this::watchLoop);
        uploaderThread = Thread.ofVirtual().name("gcs-uploader").start(this::uploadLoop);
        log.info("GCS uploader started — target: gs://{}/{}", bucketName,
                objectPrefix.isBlank() ? "" : objectPrefix + "/");
    }

    private void registerAndScanTenantDir(Path tenantDir) throws IOException {
        tenantDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        try (Stream<Path> stream = Files.list(tenantDir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".parquet"))
                    .filter(Files::isRegularFile)
                    .forEach(p -> {
                        try {
                            queue.put(p);
                            log.info("Queued existing .parquet for upload: {}", p);
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
                        if (name.toString().endsWith(".parquet") && Files.exists(fullPath)) {
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

    private void uploadLoop() {
        try {
            while (true) {
                Path path = queue.take();
                if (POISON_PILL.equals(path)) break;
                upload(path);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void upload(Path parquetFile) {
        if (!Files.exists(parquetFile)) {
            return;
        }

        String tenantId = parquetFile.getParent().getFileName().toString();
        String filename = parquetFile.getFileName().toString();
        String objectName = (objectPrefix.isBlank() ? "" : objectPrefix + "/")
                + "tenant_id=" + tenantId + "/" + filename;

        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType("application/octet-stream")
                .build();

        try {
            storage.createFrom(blobInfo, parquetFile);
            log.info("Uploaded {} -> gs://{}/{}", parquetFile.getFileName(), bucketName, objectName);
            Files.deleteIfExists(parquetFile);
        } catch (IOException | StorageException e) {
            log.error("Failed to upload {} to GCS — file remains on disk for manual retry",
                    parquetFile.getFileName(), e);
        }
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (watchService != null) {
            try { watchService.close(); } catch (IOException e) { log.warn("Error closing watcher", e); }
        }
        if (watcherThread != null) watcherThread.join(5_000);
        if (uploaderThread != null) {
            queue.put(POISON_PILL);
            uploaderThread.join(120_000);
        }
    }
}
