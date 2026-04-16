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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;

@Service("gcsUploadService")
public class GcsUploadService {

    private static final Logger log = LoggerFactory.getLogger(GcsUploadService.class);
    private static final Path POISON_PILL = Path.of("__SHUTDOWN__");

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
    private volatile Thread uploaderThread;

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
        // if credentialsPath is blank, the client uses Application Default Credentials
        // (GOOGLE_APPLICATION_CREDENTIALS env var, gcloud auth, or instance metadata)
        storage = builder.build().getService();

        uploaderThread = Thread.ofVirtual().name("gcs-uploader").start(this::runLoop);
        log.info("GCS uploader started — target: gs://{}/{}", bucketName,
                objectPrefix.isBlank() ? "" : objectPrefix + "/");
    }

    /**
     * Enqueues a parquet file for upload. No-op when GCS is disabled.
     * Called from the converter thread after each successful conversion.
     */
    public void enqueueForUpload(Path parquetFile) {
        if (!enabled) return;
        try {
            queue.put(parquetFile);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runLoop() {
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
        String objectName = objectPrefix.isBlank()
                ? parquetFile.getFileName().toString()
                : objectPrefix + "/" + parquetFile.getFileName().toString();

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
        if (uploaderThread != null) {
            queue.put(POISON_PILL);
            uploaderThread.join(120_000);  // large parquet files can be slow to upload
        }
    }
}
