package com.example.httpparquet;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;

@RestController
public class IngestionController {

    private final IngestionQueue ingestionQueue;
    private final ObjectMapper objectMapper;

    public IngestionController(IngestionQueue ingestionQueue, ObjectMapper objectMapper) {
        this.ingestionQueue = ingestionQueue;
        this.objectMapper = objectMapper;
    }

    @PostMapping(value = "/ingest", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void ingest(@RequestBody String body) {
        JsonNode tree;
        try {
            tree = objectMapper.readTree(body);
        } catch (JacksonException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid JSON: " + e.getMessage());
        }

        List<String> batch;
        if (tree instanceof ObjectNode) {
            batch = List.of(tree.toString());  // normalize to compact JSON so embedded newlines don't corrupt NDJSON
        } else if (tree instanceof ArrayNode array) {
            batch = new ArrayList<>(array.size());
            for (JsonNode element : array) {
                if (!(element instanceof ObjectNode)) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                            "Array elements must be JSON objects");
                }
                batch.add(element.toString());
            }
        } else {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Body must be a JSON object or array of objects");
        }

        try {
            ingestionQueue.put(batch);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Server shutting down");
        }
    }
}
