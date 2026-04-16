package com.example.httpparquet;

import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class ConversionQueue {

    private final LinkedBlockingQueue<Path> queue = new LinkedBlockingQueue<>();

    public void put(Path path) throws InterruptedException {
        queue.put(path);
    }

    public Path take() throws InterruptedException {
        return queue.take();
    }

    public int size() {
        return queue.size();
    }
}
