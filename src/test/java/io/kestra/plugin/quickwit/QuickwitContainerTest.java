package io.kestra.plugin.quickwit;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.quickwit.ingest.Commit;
import io.kestra.plugin.quickwit.ingest.Ingest;
import io.kestra.plugin.quickwit.search.Search;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Live round-trip against a real Quickwit node.
 *
 * <p>WireMock covers every task including the error paths, but stubs cannot catch API drift (endpoint
 * shapes, commit visibility, sort and timestamp semantics). This single test creates an index, ingests
 * two documents with {@code FORCE} commit and reads them back, proving the wiring against Quickwit
 * itself.
 *
 * <p>The test starts its own container, so it needs Docker. Where Docker is unavailable (or
 * {@code QUICKWIT_IT_URL} points at an already running node, e.g.
 * {@code QUICKWIT_IT_URL=http://localhost:7280}) the container is skipped and the given node is used
 * instead; when neither is available the test aborts instead of failing.
 */
@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QuickwitContainerTest {
    private static final GenericContainer<?> QUICKWIT = new GenericContainer<>(DockerImageName.parse("quickwit/quickwit:latest"))
        .withCommand("run")
        .withEnv("QW_CLUSTER_ID", "quickwit-it")
        .withEnv("QW_NODE_ID", "quickwit-node-1")
        .withEnv("QW_PEER_SEEDS", "quickwit-node-1")
        .withExposedPorts(7280)
        .waitingFor(Wait.forHttp("/api/v1/cluster").forPort(7280).forStatusCode(200))
        .withStartupTimeout(Duration.ofMinutes(2));

    private String baseUrl;

    @Inject
    RunContextFactory runContextFactory;

    @BeforeAll
    void startQuickwit() {
        String override = System.getenv("QUICKWIT_IT_URL");
        if (override != null && !override.isBlank()) {
            baseUrl = override;
            return;
        }
        try {
            QUICKWIT.start();
        } catch (Exception e) {
            abort("Docker is unavailable, skipping live Quickwit test: " + e.getMessage());
            return;
        }
        baseUrl = "http://" + QUICKWIT.getHost() + ":" + QUICKWIT.getMappedPort(7280);
    }

    @AfterAll
    void stopQuickwit() {
        if (System.getenv("QUICKWIT_IT_URL") == null && QUICKWIT.isRunning()) {
            QUICKWIT.stop();
        }
    }

    @Test
    void ingestThenSearchRoundTrip() throws Exception {
        String index = "kestra-it-" + UUID.randomUUID().toString().substring(0, 8).toLowerCase();
        var runContext = runContextFactory.of();

        try {
            var created = io.kestra.plugin.quickwit.index.Create.builder()
                .url(Property.ofValue(baseUrl))
                .index(Property.ofValue(index))
                .configVersion(Property.ofValue("0.8"))
                .docMapping(Property.ofValue(Map.of(
                    "mode", "lenient",
                    "timestamp_field", "timestamp",
                    "field_mappings", List.of(
                        Map.of(
                            "name", "timestamp",
                            "type", "datetime",
                            "input_formats", List.of("unix_timestamp"),
                            "output_format", "unix_timestamp_secs",
                            "fast", true
                        ),
                        Map.of("name", "severity", "type", "text", "fast", true),
                        Map.of("name", "message", "type", "text")
                    )
                )))
                .searchSettings(Property.ofValue(Map.of("default_search_fields", List.of("message"))))
                .build()
                .run(runContext);
            assertThat(created.getIndex(), is(index));

            long now = Instant.now().getEpochSecond();
            var ingested = Ingest.builder()
                .url(Property.ofValue(baseUrl))
                .index(Property.ofValue(index))
                .commit(Property.ofValue(Commit.FORCE))
                .from(Property.ofValue(List.of(
                    Map.of("timestamp", now, "severity", "ERROR", "message", "container probe one"),
                    Map.of("timestamp", now, "severity", "ERROR", "message", "container probe two")
                )))
                .build()
                .run(runContext);
            // the live API only guarantees num_docs_for_processing on success; the ingested /
            // rejected counters appear in the stubs but Quickwit 0.8.2 omits them here, so the
            // searchable-documents assertion below is the real proof the ingest landed
            assertThat(ingested.getNumDocsForProcessing(), is(2L));

            // even with FORCE the commit is asynchronous: poll until both documents are searchable
            Search.Output found = null;
            for (int i = 0; i < 30; i++) {
                var result = Search.builder()
                    .url(Property.ofValue(baseUrl))
                    .index(Property.ofValue(index))
                    .query(Property.ofValue("severity:ERROR"))
                    .maxHits(Property.ofValue(10))
                    .build()
                    .run(runContext);
                if (result.getTotal() != null && result.getTotal() >= 2L) {
                    found = result;
                    break;
                }
                Thread.sleep(2000);
            }
            assertThat(found != null, is(true));
            assertThat(found.getTotal(), is(greaterThanOrEqualTo(2L)));
            assertThat(found.getRows(), hasSize(greaterThanOrEqualTo(2)));
        } finally {
            try {
                io.kestra.plugin.quickwit.index.Delete.builder()
                    .url(Property.ofValue(baseUrl))
                    .index(Property.ofValue(index))
                    .build()
                    .run(runContext);
            } catch (Exception ignored) {
                // best-effort cleanup so repeated runs never collide
            }
        }
    }
}
