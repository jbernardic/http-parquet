package com.example.httpparquet;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;

@Configuration
public class AppConfig {

    @Value("${ingestion.output-dir}")
    private String outputDir;

    @Bean
    public Path outputDirectory() throws IOException {
        Path dir = Path.of(outputDir);
        Files.createDirectories(dir);
        return dir;
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
