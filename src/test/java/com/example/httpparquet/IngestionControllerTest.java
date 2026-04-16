package com.example.httpparquet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class IngestionControllerTest {

    @Mock
    IngestionQueue ingestionQueue;

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        IngestionController controller = new IngestionController(ingestionQueue, new ObjectMapper());
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void acceptsSingleObject() throws Exception {
        mvc.perform(post("/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"a\":1}"))
                .andExpect(status().isAccepted());

        verify(ingestionQueue).put(argThat(batch -> batch.size() == 1));
    }

    @Test
    void normalizesPrettyPrintedSingleObject() throws Exception {
        // A pretty-printed body contains literal newlines. The controller must serialize
        // via Jackson (not pass the raw string) so the JSONL line has no embedded newlines.
        String prettyJson = "{\n  \"a\": 1\n}";
        mvc.perform(post("/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(prettyJson))
                .andExpect(status().isAccepted());

        verify(ingestionQueue).put(argThat(batch ->
                batch.size() == 1 && !batch.get(0).contains("\n")));
    }

    @Test
    void acceptsArrayOfObjects() throws Exception {
        mvc.perform(post("/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("[{\"a\":1},{\"b\":2}]"))
                .andExpect(status().isAccepted());

        // One queue op, batch contains 2 records
        verify(ingestionQueue).put(argThat(batch -> batch.size() == 2));
    }

    @Test
    void rejectsMalformedJson() throws Exception {
        mvc.perform(post("/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("not-json"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsScalarBody() throws Exception {
        mvc.perform(post("/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("\"hello\""))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsWrongContentType() throws Exception {
        mvc.perform(post("/ingest")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("{\"a\":1}"))
                .andExpect(status().isUnsupportedMediaType());
    }

    @Test
    void rejectsArrayWithNonObjectElements() throws Exception {
        mvc.perform(post("/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("[{\"a\":1}, \"not-an-object\"]"))
                .andExpect(status().isBadRequest());
    }
}
