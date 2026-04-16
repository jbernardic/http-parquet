package com.example.httpparquet;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Component
public class IngestionQueue {

    private final LinkedBlockingQueue<List<String>> queue = new LinkedBlockingQueue<>();

    public void put(List<String> batch) throws InterruptedException {
        queue.put(batch);
    }

    public List<String> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public int size() {
        return queue.size();
    }
}
