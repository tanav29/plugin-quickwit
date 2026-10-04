package io.kestra.plugin.quickwit.search;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.plugin.quickwit.AbstractQuickwitTask;
import io.kestra.plugin.quickwit.QuickwitService;
import io.kestra.plugin.quickwit.models.SearchQuery;
import io.kestra.plugin.quickwit.models.SearchResult;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Fires a flow when new documents matching a query show up in a Quickwit index.
 *
 * <p>The trigger polls Quickwit on every {@code interval}. It remembers, in the namespace KV Store, the
 * timestamp up to which documents have already been delivered, and each poll searches
 * {@code timestamp >= watermark} before advancing it to {@code max(delivered timestamp) + 1}.
 * Polls sort ascending on {@code timestampField} so documents beyond {@code maxHits} are delivered on
 * a later poll instead of being skipped.
 *
 * <p>Late documents whose event timestamp is already below the watermark are not picked up again,
 * and documents sharing the boundary second of a burst larger than {@code maxHits} may be skipped:
 * raise {@code maxHits} when bursts share a timestamp.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on new Quickwit documents",
    description = """
        Polls a Quickwit index on a fixed interval and fires an execution as soon as documents matching
        the query appear.

        The last delivered timestamp is kept in the namespace KV Store and advanced to one past the
        maximum delivered timestamp on every successful poll, so a given document is only ever
        delivered once.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Alert when new errors are logged",
            full = true,
            code = """
                id: quickwit_error_watch
                namespace: company.team

                triggers:
                  - id: on_errors
                    type: io.kestra.plugin.quickwit.search.Trigger
                    url: "https://quickwit.example.com:7280"
                    index: app-logs
                    query: "severity:ERROR"
                    timestampField: timestamp
                    interval: PT5M

                tasks:
                  - id: notify
                    type: io.kestra.plugin.core.log.Log
                    message: "New errors: {{ trigger.documents | length }}"
                """
        ),
        @Example(
            title = "React to new errors on a Quickwit cluster protected by a gateway",
            full = true,
            code = """
                id: quickwit_error_watch_private
                namespace: company.team

                triggers:
                  - id: on_errors
                    type: io.kestra.plugin.quickwit.search.Trigger
                    url: "https://quickwit.example.com:7280"
                    headers:
                      Authorization: "Bearer {{ secret('QUICKWIT_GATEWAY_TOKEN') }}"
                    index: app-logs
                    query: "severity:ERROR AND service:checkout"
                    timestampField: timestamp
                    interval: PT5M
                    maxHits: 500

                tasks:
                  - id: notify
                    type: io.kestra.plugin.core.log.Log
                    message: "New checkout errors: {{ trigger.documents | length }}"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output> {
    private static final String WATERMARK_DESCRIPTION = "Last timestamp delivered by the Quickwit search trigger";

    @Builder.Default
    private Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Quickwit URL",
        description = """
            Base URL of the Quickwit REST API, including the scheme. The `api/v1` prefix is added automatically.
            A local node listens on port 7280, e.g. `http://localhost:7280`.
            """
    )
    @PluginProperty(group = "connection")
    private Property<String> url;

    @Schema(
        title = "Basic authentication",
        description = """
            Optional HTTP basic authentication credentials.

            Quickwit itself has no authentication layer, so this is only needed when the cluster sits
            behind a reverse proxy or an API gateway that enforces basic auth.
            """
    )
    @Valid
    @ToString.Exclude
    @PluginProperty(group = "connection")
    private AbstractQuickwitTask.BasicAuth basicAuth;

    @Schema(
        title = "Custom HTTP headers",
        description = """
            Headers sent on every poll, for example an `Authorization: Bearer ...` token or a gateway API key.

            Quickwit itself has no authentication layer, so this is only needed when the cluster sits
            behind an API gateway that injects credentials.
            """
    )
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<Map<String, String>> headers;

    @Schema(
        title = "Connect timeout",
        description = """
            Maximum time to wait to establish a connection to Quickwit, e.g. `PT10S`.
            When unset, the default of the underlying HTTP client applies.
            """
    )
    @PluginProperty(group = "execution")
    private Property<Duration> connectTimeout;

    @Schema(
        title = "Read timeout",
        description = """
            Maximum time to keep waiting for data on an idle connection, e.g. `PT5M`.
            Raise it when the index is large enough for a poll to take a while.
            """
    )
    @PluginProperty(group = "execution")
    private Property<Duration> readTimeout;

    @Schema(
        title = "Index ID",
        description = "ID of the index to watch."
    )
    @PluginProperty(group = "main")
    private Property<String> index;

    @Schema(
        title = "Query",
        description = """
            Query selecting the documents to react to, using the
            [Quickwit query language](https://quickwit.io/docs/reference/query-language), for example `severity:ERROR`.
            """
    )
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "Timestamp field",
        description = """
            Document field holding the event timestamp, used to advance the watermark.
            This should be the timestamp field of the index so `start_timestamp` pruning aligns.
            Its `output_format` must be `rfc3339` (the default) or `unix_timestamp_secs`, the two formats the watermark can parse.
            Polls sort ascending on this field and advance to one past the maximum delivered value.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> timestampField;

    @Schema(
        title = "Maximum number of hits",
        description = """
            Maximum number of documents to fetch per poll. Quickwit defaults to `20`.

            Raise it when documents are ingested in bursts. Polls sort ascending on `timestampField`,
            so documents beyond this limit are delivered on a later poll instead of being skipped,
            unless a single second holds more documents than this limit.
            """
    )
    @PluginProperty(group = "processing")
    private Property<Integer> maxHits;

    @Schema(
        title = "State key",
        description = """
            KV Store key holding the watermark of this trigger.
            Defaults to `<namespace>`, `<flowId>` and `<triggerId>`, each prefixed by its length, e.g. `7-company_7-my_flow_2-on`.
            Change it only to deliberately share a watermark between triggers.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        String rTimestampField = QuickwitService.requireNonBlank(
            runContext.render(this.timestampField).as(String.class).orElse(null),
            "timestampField"
        );
        SearchQuery query = searchQuery(runContext, rTimestampField);
        String key = stateKey(runContext, context);

        // absent watermark => first poll, which deliberately returns the whole current result set
        Optional<Long> watermark = readWatermark(runContext, key);

        SearchResult result;
        var configuration = QuickwitService.httpConfiguration(runContext, this.connectTimeout, this.readTimeout, this.basicAuth);

        try (var client = new HttpClient(runContext, configuration)) {
            result = QuickwitService.search(runContext, client, this.url, this.headers, query, watermark.orElse(null));
        }

        List<Map<String, Object>> documents = result.getHits() != null ? result.getHits() : List.of();
        if (documents.isEmpty()) {
            logger.debug("No new document matching the query on index '{}'", query.index());
            return Optional.empty();
        }

        long max = maxTimestamp(documents, rTimestampField);
        long advanced = max + 1;

        // When Quickwit truncated the page, it may end in the middle of second `max`: hold that second
        // back and re-read it whole on the next poll, unless the page holds nothing but that second.
        if (result.getNumHits() != null && result.getNumHits() > documents.size()) {
            List<Map<String, Object>> complete = new ArrayList<>();
            for (int i = 0; i < documents.size(); i++) {
                if (timestamp(documents.get(i), rTimestampField, i) < max) {
                    complete.add(documents.get(i));
                }
            }
            if (!complete.isEmpty()) {
                documents = complete;
                advanced = max;
            }
        }

        logger.info("Triggering on {} new document(s) on index '{}'", documents.size(), query.index());

        Output output = Output.builder()
            .documents(documents)
            .numHits(result.getNumHits() != null ? result.getNumHits() : (long) documents.size())
            .elapsedTimeMicros(result.getElapsedTimeMicros())
            .watermark(advanced)
            .build();

        // generate the execution first so a failure there does not advance the watermark past undelivered documents
        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);
        writeWatermark(runContext, key, advanced);

        return Optional.of(execution);
    }

    private SearchQuery searchQuery(RunContext runContext, String rTimestampField) throws IllegalVariableEvaluationException {
        String rIndex = QuickwitService.requireNonBlank(runContext.render(this.index).as(String.class).orElse(null), "index");
        String rQuery = QuickwitService.requireNonBlank(runContext.render(this.query).as(String.class).orElse(null), "query");
        Integer rMaxHits = QuickwitService.requireAtLeast(runContext.render(this.maxHits).as(Integer.class).orElse(null), 1, "maxHits");

        return new SearchQuery(rIndex, rQuery, null, null, null, rMaxHits, null, null, List.of("-" + rTimestampField), null);
    }

    private String stateKey(RunContext runContext, TriggerContext context) throws IllegalVariableEvaluationException {
        return runContext.render(this.stateKey)
            .as(String.class)
            .orElseGet(() -> defaultKey(context.getNamespace(), context.getFlowId(), this.id));
    }

    /**
     * Reads the last delivered timestamp.
     *
     * <p>A missing watermark means the next poll starts from the beginning. Any other KV failure
     * fails the evaluation so a poll never falls back to a full re-scan during a KV outage.
     */
    private Optional<Long> readWatermark(RunContext runContext, String key) throws Exception {
        try {
            return runContext.namespaceKv(runContext.flowInfo().namespace())
                .getValue(key)
                .map(value -> Long.parseLong(new String((byte[]) value.value(), StandardCharsets.UTF_8).trim()));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to read the Quickwit search trigger watermark '" + key + "', failing the poll: " + e.getMessage(), e);
        }
    }

    /**
     * Maximum event timestamp across the delivered documents.
     *
     * @throws IllegalStateException when a document misses the timestamp field or holds an unparsable value
     */
    private static long maxTimestamp(List<Map<String, Object>> documents, String timestampField) {
        long max = Long.MIN_VALUE;

        for (int i = 0; i < documents.size(); i++) {
            max = Math.max(max, timestamp(documents.get(i), timestampField, i));
        }

        return max;
    }

    /**
     * Event timestamp of a document, in seconds.
     *
     * <p>Quickwit renders datetime fields with their {@code output_format}: an RFC 3339 string by default, or
     * a number for {@code unix_timestamp_secs}. Other numeric output formats (millis, micros, nanos) are not
     * supported since they cannot be told apart from seconds.
     */
    private static long timestamp(Map<String, Object> document, String timestampField, int position) {
        Object value = document.get(timestampField);
        if (value == null) {
            throw new IllegalStateException("Document at position " + position + " misses timestamp field '" + timestampField + "', cannot advance the watermark");
        }

        if (value instanceof Number number) {
            return number.longValue();
        }

        String text = String.valueOf(value).trim();
        try {
            return OffsetDateTime.parse(text).toEpochSecond();
        } catch (DateTimeParseException e) {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException ignored) {
                throw new IllegalStateException(
                    "Document at position " + position + " holds timestamp field '" + timestampField + "' = '" + text +
                        "', expected an RFC 3339 datetime or a number of seconds; set the field's `output_format` to `rfc3339` or `unix_timestamp_secs`",
                    e
                );
            }
        }
    }

    private void writeWatermark(RunContext runContext, String key, long watermark) throws Exception {
        runContext.namespaceKv(runContext.flowInfo().namespace()).put(
            key,
            new KVValueAndMetadata(
                new KVMetadata(WATERMARK_DESCRIPTION, (Duration) null),
                Long.toString(watermark).getBytes(StandardCharsets.UTF_8)
            )
        );
    }

    /**
     * Default watermark key. Each part is length-prefixed so that, for example, namespace {@code a_b} with
     * flow {@code c} and namespace {@code a} with flow {@code b_c} get different keys. Only {@code -} and
     * {@code _} are used as separators since a KV key must match {@code [a-zA-Z0-9][a-zA-Z0-9._-]*}.
     */
    private static String defaultKey(String namespace, String flowId, String triggerId) {
        return String.format("%d-%s_%d-%s_%d-%s", namespace.length(), namespace, flowId.length(), flowId, triggerId.length(), triggerId);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "New documents",
            description = "Documents matching the query that no previous poll had delivered."
        )
        private List<Map<String, Object>> documents;

        @Schema(
            title = "Total matching documents",
            description = "Total number of documents matching the query over the searched window."
        )
        private Long numHits;

        @Schema(
            title = "Processing time",
            description = "Time taken by Quickwit to process the query, in microseconds."
        )
        private Long elapsedTimeMicros;

        @Schema(
            title = "Watermark",
            description = "Timestamp up to which documents have now been delivered, in seconds."
        )
        private Long watermark;
    }
}