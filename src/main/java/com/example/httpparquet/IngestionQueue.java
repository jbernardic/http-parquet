package com.example.httpparquet;

import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Component
public class IngestionQueue {

    private final LinkedBlockingQueue<TenantBatch> queue = new LinkedBlockingQueue<>();

    public void put(TenantBatch batch) throws InterruptedException {
        queue.put(batch);
    }

    public TenantBatch poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public int size() {
        return queue.size();
    }
}
