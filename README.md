# Track Picture Service

A Spring Boot service that consumes track updates (aircraft, UAVs, missiles) from the Kafka topic
`track-updates`. It keeps the **current picture** of every track and its **full history** in
PostgreSQL, and serves both through a REST API. A separate demo producer publishes moving tracks,
including a duplicate, an out-of-order update and invalid messages, so the handling can be seen
working.

The step-by-step build log, with every decision and lesson, is in [docs/PLAN.md](docs/PLAN.md).

## Requirements checklist

| Requirement | Status |
|---|---|
| Consume `track-updates`; validate; reject invalid updates with a visible reason | ✅ log line with the reason |
| Picture: latest state per track, `GET /picture`, `GET /tracks/{trackId}` | ✅ |
| History: every accepted update, `GET /tracks/{trackId}/history?from&to` | ✅ |
| Ordering: an older update never overwrites a newer state | ✅ |
| Idempotency: redelivery/replay doesn't corrupt the picture or duplicate history | ✅ |
| Producer with a duplicate, an out-of-order and an invalid message | ✅ `producer/`, a standalone app |
| *Stretch:* dead-letter topic | ✅ `track-updates-dlt`, original payload plus the reason in headers |
| *Stretch:* stale/dropped tracks | ✅ `STALE` after 10 s, removed from the picture after 30 s (configurable) |
| *Stretch:* rebuild the picture from the topic on restart | ✅ in a different form: the picture is stored in PostgreSQL, and on restart the consumer resumes from its committed offset and catches up; `/health/readiness` stays "not ready" until it has (see [After a restart](#after-a-restart)) |
| *Stretch:* tests for validation and ordering/idempotency | ✅ 118 service tests and 32 producer tests |
| *Stretch:* health endpoint with consumer lag | ✅ `/health` → `consumerLag`, and the lag as a Prometheus time series (see [Metrics](#metrics)) |

## Quick start

Requires Java 21 and Docker. Use three terminals:

```bash
make infra    # Postgres + Kafka + Prometheus, creates the topics, returns when ready  (docker compose up -d --wait)
make server   # the service on http://localhost:8080                       (./mvnw spring-boot:run)
make sim      # the demo feed: 3 tracks, 30 ticks, 1 s apart               (see `make sim-help`)
```

`make sim ARGS="--ticks=0"` runs until Ctrl+C. After you stop it, the tracks turn `STALE` after
10 s and disappear from the picture after 30 s. `make test` runs all the tests. `make infra-stop`
stops the containers; the database stays in `./postgres-data`. Prometheus is at
http://localhost:9090 (see [Metrics](#metrics)).

What to look for:

```text
# producer                                            # service
tick 3: sent DUPLICATE for TRK-001                    DUPLICATE trackId=TRK-001 timestamp=…:13Z
tick 5: sent OUT_OF_ORDER for TRK-002                 STALE     trackId=TRK-002 timestamp=…:14Z
tick 7: sent INVALID for TRK-003 — latitude 91        Rejected track update [… key=TRK-003]: invalid fields:
                                                        latitude must be less than or equal to 90.0 (was 91.0)
tick 7: sent INVALID for TRK-003 — identity ENEMY     Rejected track update [… key=TRK-003]: unparseable payload: …
```

## Architecture

```
 producer/ (standalone)                                  PostgreSQL
     │ key = trackId, JSON                         ┌──────────────────────┐
     ▼                                             │ track_history        │ append-only, unique(track_id, observed_at)
 [track-updates] 3 partitions                      │ track_picture        │ one row per track, status ACTIVE/STALE
     │                                             └──────────────────────┘
     ▼                                                  ▲           ▲   ▲
 TrackUpdateListener ── invalid ──► log + [track-updates-dlt]       │   │
     │ @Valid TrackUpdate                               │           │   │
     ▼                                                  │           │   │
 TrackUpdateService ── one transaction ─────────────────┘           │   │
     1. history: INSERT … ON CONFLICT DO NOTHING  (→ DUPLICATE)     │   │
     2. picture: UPSERT … WHERE stored < incoming (→ STALE)         │   │
                                                                    │   │
 PictureExpiryJob (every 2 s, also at startup): mark STALE / drop  ─┘   │
 REST: /picture  /tracks/{id}  /tracks/{id}/history  /health  /metrics  ┘
 Prometheus (compose, :9090) ◄── scrapes /prometheus every 5 s
```

It's one deployable with three packages: `ingest` (Kafka → service), `picture` and `history`, each
with its own entity, repository and controller. The producer is a separate plain-Java project,
with no shared code.

## Topic and message design

- **Key = `trackId`.** All updates of a track land on the same partition, so Kafka delivers them
  in order, while different tracks are processed in parallel (3 partitions, 3 consumer threads).
- **JSON value**, matching the contract: `trackId`, `timestamp` (ISO-8601 UTC), `latitude`, `longitude`,
  optional `altitude` (≥ 0) and `heading` (0 ≤ h < 360), and `identity`
  (`UNKNOWN|FRIEND|NEUTRAL|HOSTILE`).
- **Consumed as a String and converted at the listener**, not in the Kafka deserializer. That way
  the record keeps the producer's exact bytes, and the **DLT receives the original payload** for
  every kind of rejection (bad JSON, unknown enum, out-of-range value), with the exception in
  `kafka_dlt-*` headers.
- **Topics belong to the infrastructure.** `compose.yaml` creates them; the broker doesn't
  auto-create topics, and the service refuses to start if one is missing (`missing-topics-fatal`).
  This avoids a silently auto-created topic with one partition.

## Ordering, duplicates and replay

| Situation | What happens | Outcome |
|---|---|---|
| New, newest update | appended to history, replaces the picture row | `APPLIED` |
| Out of order (older than the stored state) | appended to history, picture unchanged | `STALE` |
| Exact duplicate (redelivery, replay, producer retry) | history's unique `(track_id, observed_at)` rejects it, so the picture is never touched | `DUPLICATE` |
| Same timestamp, different content | same observation: the first version wins | `DUPLICATE` |
| Update already older than drop-after (30 s) | history only, so a dropped track can't come back with old data | `STALE` |
| Invalid values / unparseable | never stored; log line + DLT | rejected |
| DB down or other unexpected error | retried with back-off (0.5 s … 30 s, no limit); the partition waits, so ordering holds | retried |

Two details make this safe under concurrency and crashes:

- **Each write is a single atomic SQL statement.** The picture update is
  `INSERT … ON CONFLICT DO UPDATE … WHERE track_picture.observed_at < EXCLUDED.observed_at`. The
  database compares and writes in one step, so there's no read-compare-write race. History uses
  `ON CONFLICT DO NOTHING` instead of catching an exception, because a JPA constraint violation
  would mark the whole transaction for rollback.
- **History and picture are written in one transaction**, so "in history" means "already applied
  to the picture". If the service crashes after the commit but before the Kafka offset commit, the
  redelivered record is detected as a duplicate.

Ordering uses the sensor's **`timestamp`**, never arrival time. History also stores `receivedAt`,
so late arrivals are visible. In this excerpt, `…:14Z` arrived after `…:15Z`:

```json
{"timestamp":"2026-11-16T08:36:14Z","latitude":37.999347,"receivedAt":"2026-11-16T08:36:16.065841Z"}
{"timestamp":"2026-11-16T08:36:15Z","latitude":37.999184,"receivedAt":"2026-11-16T08:36:16.053786Z"}
```

### After a restart

The consumer group's committed offsets are stored in Kafka, so on restart the consumer **resumes
right after the last committed record** and processes everything published while it was down, in
order per track. That catch-up is what rebuilds the picture. At startup, the expiry job drops
tracks silent for more than 30 s. The backlog then re-adds those observed within the last 30 s;
older backlog entries go to history only. Records processed but not committed before a crash are
delivered again and come out as `DUPLICATE`.

While catching up, the picture is behind, so **`/health/readiness` reports `OUT_OF_SERVICE`
(HTTP 503) until the lag drops to 10 records** (`trackstore.ingestion.ready-max-lag`). A load
balancer or Kubernetes then holds traffic back without restarting anything. From a real run with
9,003 records waiting:

```text
11:56:29 HTTP 503 {"status":"OUT_OF_SERVICE","catchUp":{"caughtUp":false,"totalLag":9003,"readyAtLag":10}}
11:56:34 HTTP 503 {"status":"OUT_OF_SERVICE","catchUp":{"caughtUp":false,"totalLag":5476,"readyAtLag":10}}
11:56:38 HTTP 200 {"status":"UP","catchUp":{"caughtUp":true}}
```

Once caught up, the instance stays ready: a later burst of lag shouldn't take every instance out of
service at the same time, and it stays visible in `consumerLag`. Liveness never depends on Kafka,
because restarting a lagging consumer only makes it slower.

## Read API

| Endpoint | Response |
|---|---|
| `GET /picture` | All tracks, sorted by `trackId` |
| `GET /tracks/{trackId}` | The latest state; **404** if unknown or dropped |
| `GET /tracks/{trackId}/history?from=&to=` | Chronological; `from` inclusive, `to` exclusive, both optional ISO-8601. Unknown track → **404**; known track with nothing in range → `200 []`; `from` after `to` or a bad date → **400** |
| `GET /health`, `GET /metrics` | Actuator. `consumerLag` shows the lag per partition for the whole consumer group |
| `GET /prometheus` | All metrics in Prometheus format, scraped by the Prometheus in `compose.yaml` |
| `GET /health/readiness`, `GET /health/liveness` | Probes. Readiness includes `catchUp` (503 until the backlog after a restart is consumed); liveness doesn't depend on Kafka |

Any read endpoint returns **503** with `Retry-After: 5` if the database is unreachable (see below).

```json
GET /tracks/TRK-002
{"trackId":"TRK-002","timestamp":"2026-11-16T08:36:19Z","latitude":37.998532,"longitude":23.803657,
 "altitude":1500.0,"heading":117.0,"identity":"UNKNOWN","lastReceivedAt":"2026-11-16T08:36:20.207805Z","status":"ACTIVE"}

GET /tracks/NOPE   → 404, application/problem+json
{"detail":"No track with id 'NOPE'","instance":"/tracks/NOPE","status":404,"title":"Not Found"}
```

Responses are DTOs, not entities, so the JSON contract doesn't change when a table does. Errors
follow RFC 9457 (Problem Details). The read services throw domain exceptions
(`TrackNotFoundException`) that know nothing about HTTP; one `@RestControllerAdvice`,
`ApiExceptionHandler`, maps them (and database outages) to status codes.

### If the database is down

At first, stopping PostgreSQL made every request hang for **30 s** (Hikari's default wait for a
pooled connection) and then fail with a generic **500**. Now the pool waits at most 2 s
(`spring.datasource.hikari.connection-timeout`), and `ApiExceptionHandler` turns
"no connection" into a 503 the client can act on. Measured with `docker compose stop postgres`:

```
GET /picture             → 503 in 2.03 s     (before: 500 after 30.07 s)
Retry-After: 5
Content-Type: application/problem+json
{"detail":"The track database is unavailable. Try again shortly.","instance":"/picture",
 "status":503,"title":"Service Unavailable","dependency":"database"}
```

The body names the dependency but not the cause: exception messages contain hosts and pool
details, so they go to the log (one line per request), and `/health` shows the `db` component as
`DOWN`. After `docker compose start postgres`, the next request succeeds; no restart is needed.
Ingestion loses nothing during the outage: the listener retries the same record (unlimited
retries, see below) and continues when the database is back.

I deliberately **didn't add the database to readiness**: every instance shares it, so an outage
would take all of them out of rotation at once and the load balancer would answer with its own
error instead of this 503.

## Metrics

Besides what Spring Boot already measures (HTTP requests by status, Hikari pool, Kafka client,
`@Scheduled` runs, JVM), the service publishes its own:

| Prometheus name | Type | What it answers |
|---|---|---|
| `trackstore_updates_total{outcome}` | counter | Is data flowing? `applied` / `stale` (out of order) / `duplicate` (redelivery, replay) |
| `trackstore_updates_rejected_total{reason}` | counter | Is a producer sending bad data? `invalid_fields` / `unparseable` / `other` |
| `trackstore_updates_age_seconds` | histogram | How old is a position when it reaches the picture (now − observation timestamp)? |
| `trackstore_ingestion_lag_records{partition}` | gauge | How far behind the topic is the consumer group? |

Each answers a question that before was only in the logs or in `/health` (which shows the current
value, not its history). A few queries for http://localhost:9090 (Graph tab):

```promql
sum by (outcome) (rate(trackstore_updates_total[1m]))                                # updates/s by outcome
histogram_quantile(0.95, sum by (le) (rate(trackstore_updates_age_seconds_bucket[1m])))  # p95 age
sum(trackstore_ingestion_lag_records)                                                # total lag
```

How I kept them tidy:

- **The business logic doesn't know about metrics.** `TrackUpdateService` returns an outcome; the
  listener and the error handler (the edges) report to `IngestionMetrics`, which owns the names
  and tags. `ConsumerLagMetrics` does the same for the lag.
- **Only tags with a few fixed values** (outcome, reason, partition), never `trackId`: each tag
  value is its own time series, so per-track tags would grow without limit.
- **Every series exists from startup at 0**, so `rate()` works before the first event.
- **The lag is refreshed every 5 s, not on scrape.** Reading it means three calls to the broker,
  and a scrape shouldn't wait for a broker that is down. If the lag can't be read, its series are
  removed: a gap in the graph says "unknown", while a frozen last value would look healthy.

From a live run: the demo feed gave exactly the expected counts (299 applied, 1 duplicate, 1 stale,
1 rejected per reason). After a restart with a backlog, the lag series showed `8003`, then `0` five
seconds later. And the age metric found something I hadn't noticed: **positions were 2.8 s old on
average (max 4.2 s)**, although everything runs on one machine. The cause was the demo producer,
not the consumer: it stamped tick *n* with `start + n × interval`, and real time ran ahead of that
by the time each tick took to send (~12 ms). With `--ticks=0` the age kept rising, by ~0.7 s a
minute: after ~15 minutes "live" tracks flickered to `STALE`, and after ~40 they'd have dropped out
of the picture. The producer now stamps each tick with the clock, like a real sensor.

## Decisions and trade-offs

- **One deployable, separate modules.** It's simpler to run and reason about. Splitting picture
  and history into two services would mean two consumer groups on the same topic; each store's
  write is already idempotent on its own, so that split is mostly operational work.
- **PostgreSQL for both stores.** It's durable, the conditional upsert and range queries are
  native, and the picture survives restarts. Together with Kafka's committed offsets, a restart
  doesn't need a separate rebuild. The weak spot is that offsets live in Kafka and data lives in
  PostgreSQL: **restoring the database from an older backup** would leave a gap, because the
  consumer continues from where Kafka says it was. The fix is to reset the group's offsets to the
  backup time (`kafka-consumer-groups.sh --reset-offsets --to-datetime …`) and let idempotency
  absorb the overlap. Kafka's retention bounds how far back that works (7 days by default; the same
  default applies to committed offsets of an inactive group, after which `earliest` re-reads the
  retained topic, which is harmless thanks to idempotency).
- **One transaction for history and picture.** It gives simple, strong guarantees, but couples the
  two stores. The requirements allow eventual consistency; I took the stronger option because it
  costs nothing here.
- **Validation at the listener (`@Valid`)**, since Kafka is the only entry point. A future second
  entry point would have to validate as well. The rules mirror the DB columns (for example
  `@Size(max = 64)` ↔ `VARCHAR(64)`), so a "valid" message can't fail on insert and block its
  partition.
- **Unlimited retries for unexpected errors.** Valid data is never dropped and per-track order is
  kept. The cost is that a bug that always fails one valid record stalls its partition; that's
  visible as lag on `/health`.
- **Staleness is measured from the observation timestamp, not arrival.** After a replay or a
  backlog, old positions don't look live. Dropped tracks are deleted from the picture; their
  history stays.
- **The producer is a separate plain-Java app with its own copy of the message contract.** It
  behaves like a real external feed, but the contract is duplicated rather than shared.

## Testing

- **Unit tests:** validation rules (boundaries for every field), service decisions (Mockito),
  configuration binding, lag arithmetic, the simulator and scenario.
- **Against real PostgreSQL (Testcontainers):** the repositories' SQL (duplicates, "newer wins",
  ranges, expiry) and the service on real repositories.
- **End to end with real Kafka:** raw JSON in → picture, history and DLT out, including
  out-of-order, duplicates and unparseable messages. Also the health endpoint: a stopped listener
  turns it `DOWN`, lag builds up, and it recovers after a restart. The readiness rule ("not ready
  until caught up, then stays ready") has its own unit tests.
- **Web slice (`@WebMvcTest`):** controllers with mocked repositories that throw what Spring
  throws when PostgreSQL is down → 503 problem response, no internal details in the body. (The
  live outage above was checked by hand; stopping the shared test database would break the other
  tests.)
- **Metrics:** unit tests with an in-memory `SimpleMeterRegistry` and a fixed clock (names, tags,
  age only for applied updates, lag series removed when unknown), and an end-to-end test that the
  counters move. `/prometheus` itself was checked live: export to a monitoring system is off in
  Spring tests, and `@AutoConfigureMetrics` would start a second test context whose listeners would
  share the partitions with the first.
- **Producer:** `MockProducer` for the publishing loop, plus a real broker to check that records
  are keyed so that each track stays on one partition.

## With more time

- History **pagination/limits**, and retention or table partitioning for long-running data.
- Actuator on a **separate management port** with authentication; the API also has none today.
- **Alerts** (Prometheus rules on lag and age) and a Grafana dashboard; gauges for the picture
  (tracks by status and identity), refreshed by the expiry job rather than counted on each scrape.
- A **schema registry** (Avro/Protobuf) instead of JSON copied between producer and service, or at
  least **contract tests** between them.
- Store **partition/offset** in history, to trace any row back to its Kafka record.
- A replicated, multi-broker Kafka; CI; container images for the service and the producer.
- A **volume for Kafka** in `compose.yaml`: today `docker compose down` wipes topics and offsets,
  while PostgreSQL keeps its data in `./postgres-data`.

## How this was built

I built this with an AI coding assistant (Claude Code) as a pair programmer. I set up a step-by-step
plan ([docs/PLAN.md](docs/PLAN.md)) and took the design decisions at each step. That includes the
topic ownership, the staleness rules and dropping silent tracks, and the standalone producer. I
reviewed every step and committed each one myself. The assistant wrote most of the code and tests
and explained the Spring concepts as we went; everything was checked by running the tests and the
live demo.
