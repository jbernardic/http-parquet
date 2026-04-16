package com.example.httpparquet;

import java.util.List;

public record TenantBatch(String tenantId, List<String> records) {}
