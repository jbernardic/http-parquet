package com.example.httpparquet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

@SpringBootTest
class HttpParquetApplicationTests {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void outputDir(DynamicPropertyRegistry registry) {
        registry.add("ingestion.output-dir", tempDir::toString);
    }

    @Test
    void contextLoads() {
    }

}
