# Implementation Plan — Track Picture Service

Step-by-step plan for the take-home assignment (see [REQUIREMENTS.md](REQUIREMENTS.md)).
Each step is small, reviewed, and committed before moving on.

## Stack

- Java 21, Spring Boot 4.1 (Web MVC, Spring Data JPA, Spring for Apache Kafka)
- PostgreSQL 17 and Apache Kafka 4.1 (KRaft, single node) via Docker Compose
- Shortcuts (`Makefile`, `make` lists them): `make infra`, `make server`, `make sim ARGS="--ticks=0"`,
  `make infra-stop`, `make test`
- Run locally: `docker compose up -d --wait` (infrastructure + topics), then the service with
  `./mvnw spring-boot:run` or `java -jar target/trackstore-0.0.1-SNAPSHOT.jar`
- Demo feed (`producer/`, plain Java 21 + kafka-clients): `./mvnw -f producer/pom.xml package`, then
  `java -jar producer/target/track-producer.jar` (`help` lists the options)

## Target Design

```
 producer ──► [track-updates] (key = trackId)
                     │
              TrackUpdateListener ──invalid──► log + [track-updates-dlt]
                     │ valid
               TrackUpdateService  (one DB transaction)
                ├─► track_history  (INSERT, unique(trackId, timestamp) → duplicates skipped)
                └─► track_picture  (UPSERT only if incoming timestamp > stored timestamp)
                     │
   REST: GET /picture
         GET /tracks/{trackId}
         GET /tracks/{trackId}/history?from=&to=
```

### Key decisions

- **Ordering:** messages are keyed by `trackId`, so all updates for a track land on the same
  partition and are consumed in order. Out-of-order arrivals (older timestamp) never overwrite the
  picture — the upsert only applies when the incoming timestamp is newer.
- **Idempotency:** history has a unique constraint on `(trackId, timestamp)`; redelivered or
  replayed messages are detected and skipped, so history is never duplicated.
- **Boundaries:** one deployable with separate `picture` and `history` modules. Simpler to run
  and reason about; the README explains how they could be split into separate services.
- **Storage:** PostgreSQL for both — durable, supports the conditional upsert and range queries
  on history, and lets the picture survive restarts.

## Steps

| #  | Step | Spring concepts | Status |
|----|------|-----------------|--------|
| 1  | Dev environment: Java 21, Postgres + Kafka in Compose, app boots | Docker Compose support, `application.yaml` | ✅ Done |
| 2  | Domain model: `TrackUpdate` record, `Identity` enum, validation rules | Records, Bean Validation | ✅ Done |
| 3  | Unit tests for validation | JUnit 5, AssertJ | ✅ Done |
| 4a | History storage: Flyway schema, entity, repository, unique constraint | Flyway, JPA, Spring Data repositories | ✅ Done |
| 4b | History repository tests against real Postgres (append, duplicate, range query) | Testcontainers, `@DataJpaTest`, `@ServiceConnection` | ✅ Done |
| 5  | Picture storage with "newer timestamp wins" upsert + repository tests | `@Modifying` native queries, persistence context | ✅ Done |
| 6  | `TrackUpdateService` (history → picture, one transaction) + unit & integration tests | `@Service`, constructor injection, `@Transactional`, Mockito | ✅ Done |
| 7a | Kafka listener: `@Payload @Valid`, JSON conversion, rejected → log line + `track-updates-dlt`, retry policy; live demo | `@KafkaListener`, `KafkaListenerConfigurer`, `DefaultErrorHandler`, `DeadLetterPublishingRecoverer` | ✅ Done |
| 7b | Listener tests on real Kafka: valid/duplicate/out-of-order, invalid values, unparseable JSON (e.g. `"identity": "ENEMY"`) → DLT | Testcontainers Kafka, `@SpringBootTest`, Awaitility | ✅ Done |
| 8  | REST API: `GET /picture`, `GET /tracks/{id}`, `GET /tracks/{id}/history?from&to` (optional, `[from, to)`); 404/400 as Problem Details | `@RestController`, record DTOs, `ErrorResponseException`, RFC 9457, `MockMvcTester` | ✅ Done |
| 9  | Demo producer (`producer` profile): moving tracks + a duplicate, an out-of-order and two invalid messages | `@Profile`, `CommandLineRunner`, `KafkaTemplate`, `@ConfigurationProperties` record + `@Validated` | ✅ Done |
| 10a | Stretch: stale tracks — `STALE` after 10 s, removed from the picture after 30 s (configurable), checked at startup and every 2 s | `@Scheduled`, `@EnableScheduling`, injectable `Clock`, `ApplicationContextRunner` | ✅ Done |
| 10b | Stretch: `/health` with a `consumerLag` component (lag per partition for the whole group), `/metrics` | Actuator, custom `HealthIndicator`, Kafka `Admin`, Micrometer | ✅ Done |
| 11 | README: how to run, architecture diagram, decisions, trade-offs, future work (first person, compact, AI note) | — | ✅ Done |
| 12 | Run without Spring Boot's Docker Compose support: explicit DB config, topics created by Compose, fail fast on missing topics (see below) | Externalized config, `missing-topics-fatal`, Compose healthchecks | ✅ Done |
| 13 | Demo producer as its own plain-Java application, run independently of the service (see below); replaces the `producer` profile of step 9 | — (plain Java: `KafkaProducer`, `MockProducer`, shutdown hook) | ✅ Done |
| 14 | Readiness waits for catch-up after a restart (`catchUp` in `/health/readiness`, latching); README: restart behaviour and its limits | Health groups, readiness vs liveness probes | ✅ Done |

Order: 12 → 13 → 11. The README is written last, so it describes the final way to run everything.

### Step 12 — Run the service independently of Docker Compose

Today `application.yaml` has no database URL — it comes only from the Compose service connection,
so `java -jar` fails. Decisions: the app stays outside Docker (no app container), and Compose
creates **both** topics.

1. **Explicit config**: `spring.datasource.url/username/password` with local defaults (dev-only
   credentials), overridable via env vars (`SPRING_DATASOURCE_URL`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`).
   Remove the `spring-boot-docker-compose` dependency.
2. **Topics owned by infrastructure**: a one-shot `kafka-init` service in `compose.yaml`
   (`depends_on` Kafka *healthy*) runs `kafka-topics.sh --create --if-not-exists` for
   `track-updates` (3 partitions) and `track-updates-dlt`. Remove the `NewTopic` beans.
3. **Fail loudly on a missing topic**: broker `auto.create.topics.enable=false` (else a 1-partition
   topic appears silently); app `spring.kafka.listener.missing-topics-fatal: true`.
4. **Tests**: Testcontainers is unaffected, but relied on the `NewTopic` beans → move them into
   `KafkaTestcontainersConfiguration` (topic setup now exists in Compose and in tests).
5. **Healthchecks** for Postgres (`pg_isready`) and Kafka; start with `docker compose up -d --wait`.
6. **Named volume** for Postgres, so `docker compose down` keeps the data.
7. Update run instructions (Stack section, README, IntelliJ run configuration).

Risks: setting any `KAFKA_*` env var disables the `apache/kafka` image's defaults → the full KRaft
config must be given (node id, roles, listeners, controller quorum, advertised listeners). With
`missing-topics-fatal`, the app crashes if started before `kafka-init` ran; and nothing waits for
Postgres any more — both are deliberate fail-fast behaviour, to be documented.

### Step 13 — Producer as an independent application

Problem: the producer is a `CommandLineRunner` inside the service (`producer` profile). Runners
execute *before* the application is ready, so the service reports not-ready (`readinessState`)
until the whole demo has been published, and the producer can't run without the service.

Decisions: a **standalone project** next to the service (not a multi-module build), written in
**plain Java** with the Kafka client (no Spring).

1. **New standalone project `producer/`** with its own `pom.xml` (`dev.kostas.trackproducer`):
   `kafka-clients`, Jackson for JSON, SLF4J for logging; packaged as a runnable jar. The service's
   build is untouched.
2. **Plain Java**: a `main` that reads settings from command-line arguments with environment
   variable fallbacks (bootstrap servers, tracks, ticks, interval) and publishes with a
   `KafkaProducer<String, String>` (key = trackId, `acks=all`, idempotence on).
3. **Run modes**: a fixed number of ticks, then exit with code 0; or `ticks: 0` = run until
   Ctrl+C (a shutdown hook flushes and closes the producer) — useful to watch tracks go STALE and
   get dropped once the producer is stopped.
4. **Own message type**: the producer has its own `TrackUpdateMessage` record instead of importing
   the service's `TrackUpdate`. The JSON is the contract between them, like a real external sensor;
   the topic name is duplicated too. Trade-off: the contract is copied, not shared.
5. **Move** `TrackSimulator`, `DemoScenario` and their unit tests; remove the `producer` profile,
   `DemoProducer`, `DemoProducerProperties` and `application-producer.yaml` from the service.
6. **Tests**: `DemoScenarioTest` currently checks "valid" with the service's Bean Validation
   annotations, which the producer won't have → check the documented ranges directly.
   `DemoProducerIntegrationTest` (scenario → correct picture) needs both apps; the service's listener
   tests already cover each anomaly, so it is replaced by a producer-side test that the records reach
   a Testcontainers Kafka with the right keys.
7. **Run**: `docker compose up -d --wait`, start the service, then
   `./mvnw -f producer/pom.xml package` and `java -jar producer/target/track-producer.jar --ticks=60`.
   Doing Step 12 first means the producer never creates topics either.

## Notes & Lessons Learned

- **The service doesn't depend on Docker Compose** (Step 12; until then Spring Boot's
  `spring-boot-docker-compose` started the containers and supplied the DB connection). Connection
  settings are explicit in `application.yaml` with local defaults, overridable by environment
  variables (`SPRING_DATASOURCE_URL`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, ...).
- **Infrastructure owns the topics**: the one-shot `kafka-init` service creates them
  (`--if-not-exists`, so every `up` is safe). It uses `network_mode: service:kafka`, so
  `localhost:9092` inside it *is* the broker — avoiding the advertised-listener problem.
  `docker compose up -d --wait` waits for the healthchecks *and* for `kafka-init` to finish.
- **Fail fast on a missing topic**: broker `auto.create.topics.enable=false` (otherwise the first
  client silently creates a 1-partition topic) and `missing-topics-fatal: true` (the service exits
  at startup). Tests create the same topics in `KafkaTestcontainersConfiguration`.
- The `apache/kafka` image drops **all** its defaults once any `KAFKA_*` variable is set, so
  `compose.yaml` spells out the full single-node KRaft configuration.
- Kafka clients connect to the bootstrap address, then reconnect to the broker's *advertised*
  listener. With the same port inside and outside the container (`9092:9092`),
  `localhost:9092` works from both sides.
- Postgres is published on a **fixed host port** (`5432:5432`) so the service and DBeaver/psql can
  connect. Its data lives in `./postgres-data` (a bind mount, git-ignored), so it survives
  `docker compose down`; to start with an empty database, stop the services and delete the folder.
- **Flyway is SQL-first**: migrations are hand-written `V<n>__<name>.sql` files, applied once and
  recorded (with a checksum) in `flyway_schema_history`. Never edit an applied migration — add a
  new one. Hibernate runs with `ddl-auto: validate` as a safety net: the app fails at startup if
  entities and tables disagree. (Autogenerate à la Alembic exists via Liquibase + liquibase-hibernate
  or IntelliJ Ultimate, but generated DDL drops named constraints/defaults that matter here.)
- **Duplicates via `INSERT ... ON CONFLICT DO NOTHING`**, not `save()` + catch: a JPA constraint
  violation marks the whole transaction rollback-only, which would also undo the picture update.
- **Validation must mirror DB constraints** (`@Size(max = 64)` ↔ `VARCHAR(64)`); otherwise a message
  passes validation, fails on insert, and becomes a poison message that blocks its partition.
- **"Newer wins" is one atomic SQL statement** (`ON CONFLICT DO UPDATE ... WHERE stored < incoming`),
  not read → compare in Java → save, which has a lost-update race between concurrent writers.
  Strictly newer (`<`): an equal timestamp is the same observation → ignored, first version wins.
- **A newer update replaces the whole state**; missing optional fields become `null` rather than
  keeping stale values from an older observation.
- **History and picture are written in one transaction**, so "in history" ⇒ "applied to picture".
  History is therefore the duplicate check: a duplicate never reaches the picture. Outcomes:
  `APPLIED` (history + picture), `STALE` (out of order → history only), `DUPLICATE` (nothing).
  Trade-off: couples the two stores; if split into separate services, each would consume the topic
  in its own consumer group and rely on its own idempotent write (which both already have).
- **Validation happens at the listener** (`@Payload @Valid`, enabled via `KafkaListenerConfigurer`),
  since Kafka is the only entry point. Trade-off: a future second entry point (e.g. REST ingest)
  would have to validate too; the service itself trusts its input.
- **Consume as String, convert at the listener** (`StringJacksonJsonMessageConverter`): the record
  keeps the original text, so the DLT receives exactly what the producer sent, for every kind of
  rejection (unparseable JSON, unknown enum, invalid values). Spring adds `kafka_dlt-*` headers
  with the exception class and message.
- **Retry policy**: conversion/validation failures are non-retryable by default in Spring Kafka →
  straight to log + DLT. Any other failure (e.g. DB down) is retried with exponential back-off and
  no limit: valid data is never dropped, and the blocked partition preserves per-track ordering.
  Trade-off: a bug that fails a valid record forever stalls its partition (visible via lag).
- Tests that start the full app get their **own Kafka container**, so they never consume from
  the developer's local topic.
- **Native `@Modifying` queries bypass the persistence context** — `clearAutomatically = true`
  evicts cached entities so a later `findById` in the same transaction doesn't return stale data.
- **Read API returns DTOs, not entities** (`TrackStateResponse`, `TrackHistoryEntryResponse`): the
  JSON contract is independent of the table layout and uses the producer's field name `timestamp`.
- **Errors as RFC 9457 Problem Details** (`spring.mvc.problemdetails.enabled`): an exception that
  *is* an `ErrorResponse` (`TrackNotFoundException`, `ResponseStatusException`) or a bad parameter
  (`from=yesterday`) becomes `application/problem+json` without any handler code.
- **History filters are optional** and half-open `[from, to)`, so consecutive windows never overlap.
  Unknown track → 404; known track with an empty window → `200 []`. `from > to` → 400.
- **PostgreSQL can't type `:param is null`** ("could not determine data type of parameter"), so
  missing bounds are replaced by far-past/far-future instants in Java instead of in the query.
- **Controller tests with `@SpringBootTest` + MockMvc + `@Transactional`**: MockMvc runs the request
  in the test thread, so test data and request share one transaction that is rolled back.
  Future work: pagination/limit for long histories.
- **The demo producer was first a Spring profile** (step 9), but a `CommandLineRunner` runs *before*
  the application is ready, so the service reported not-ready for the whole demo. Step 13 moved it
  to its own plain-Java project (`producer/`): separate process, own copy of the message contract
  (`TrackUpdateMessage`), no shared code with the service — like a real external sensor feed.
- **Logic is pure, I/O is thin**: `TrackSimulator` (positions) and `DemoScenario` (which messages,
  in which order) are plain classes with unit tests; `TrackFeedPublisher` only sends and waits. It
  takes Kafka's `Producer` interface, so tests use `MockProducer` (in-memory) or a real broker.
- **Stopping cleanly**: Ctrl+C/SIGTERM run JVM shutdown hooks while `main` continues; the hook
  releases a latch that the pause between ticks waits on, then waits for the producer to be closed.
  No thread interrupts: they would make the Kafka client's `close()` fail.
- **The producer fails fast** too: `max.block.ms=10s` turns a missing broker or topic into exit
  code 1 after 10 s (default 60 s), since the broker no longer auto-creates topics.
- **Testing a background job from a shell**: a background process in a non-interactive shell
  ignores SIGINT — use SIGTERM (same shutdown hooks) to stop it.
- **Two instances in the same consumer group share the partitions** (e.g. IntelliJ + a terminal
  run): each logs only its own partitions' outcomes, but the stored result is the same.

- **Stale/dropped tracks**: a `@Scheduled` job marks tracks `STALE` (`status` column, Flyway V3)
  and deletes dropped ones from the picture — history is kept. Age = now − latest *observation*
  timestamp, so a replayed or backlogged feed doesn't look live. `fixedDelay` without initial delay
  also runs at startup, catching tracks that expired while the service was down.
- **Each expiry step is one SQL statement** (`UPDATE/DELETE ... WHERE observed_at < cutoff`): the
  condition is re-checked on the row when it is written, so a concurrent upsert can't be overwritten.
  `RETURNING track_id` (run as a query, not `@Modifying`) gives the ids for the log line.
- **Updates already past drop-after go to history only** (outcome `STALE`): otherwise, with the row
  deleted, nothing is left to compare with, and a late or replayed update would revive the track.
- **Inject a `Clock`** instead of calling `Instant.now()`: tests fix the time. Full-app tests run on
  the real clock, so their timestamps are relative to now.
- **Consumer lag = latest offset − committed offset**, per partition, read from the broker with the
  Kafka `Admin` client (like `kafka-consumer-groups.sh --describe`). Spring Boot has no Kafka health
  indicator, so `ConsumerLagHealthIndicator` is custom. It covers the **whole group**; the Kafka
  client metric `records-lag-max` only covers this instance's partitions (`NaN` when it has none).
- **High lag is still `UP`**: a lagging consumer is working off a backlog, and an orchestrator
  restarting a DOWN instance would make it worse. `DOWN` only when the broker is unreachable or a
  listener container is stopped. Lag goes in the details, for dashboards/alerts.
- `show-details: always` is for the local demo; production would use `when-authorized`.
- **Restart = catch-up**: committed offsets live in Kafka, so a restarted consumer resumes after the
  last committed record; that backlog *is* the picture rebuild. Limit: offsets (Kafka) and data
  (PostgreSQL) are separate — restoring an older DB backup needs an offset reset to the backup time.
- **Readiness ≠ liveness**: `catchUp` (in the `readiness` group only) is `OUT_OF_SERVICE` until the
  lag first drops to `ready-max-lag`, then latches `UP`, so a later lag burst can't take every
  instance out of service at once. Liveness never depends on Kafka.
