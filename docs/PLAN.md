# Implementation Plan — Track Picture Service

Step-by-step plan for the take-home assignment (see [REQUIREMENTS.md](REQUIREMENTS.md)).
Each step is small, reviewed, and committed before moving on.

## Stack

- Java 21, Spring Boot 4.1 (Web MVC, Spring Data JPA, Spring for Apache Kafka)
- PostgreSQL 17 and Apache Kafka 4.1 (KRaft, single node) via Docker Compose
- Run locally with `./mvnw spring-boot:run` — Spring Boot starts the Compose services automatically

## Target Design

```
 producer ──► [track-updates] (key = trackId)
                     │
              TrackUpdateListener ──invalid──► log + [track-updates.DLT]
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
| 11 | README: how to run, architecture diagram, decisions, trade-offs, future work | — | ⬜ |

## Notes & Lessons Learned

- `spring-boot-docker-compose` is an `optional` dependency, so it is **not** packaged into the jar.
  It is a dev-time feature: use `./mvnw spring-boot:run` (or IntelliJ) during development.
- Spring Boot auto-connects to the Postgres container, but has **no** Docker Compose service
  connection for the `apache/kafka` image — hence the fixed `9092:9092` port mapping and the
  explicit `spring.kafka.bootstrap-servers` setting.
- Kafka clients connect to the bootstrap address, then reconnect to the broker's *advertised*
  listener. With the same port inside and outside the container (`9092:9092`), the default
  `localhost:9092` works from both sides.
- Postgres is published on a **fixed host port** (`5432:5432`) so DBeaver/psql can connect.
  With the default lifecycle (`start-and-stop`) the containers stop when the app stops. To keep
  the DB up independently, run `docker compose up -d` first: if the services are already running,
  Spring Boot neither starts nor stops them. Stop them with `docker compose stop` / `down`.
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
- **The demo producer is a Spring profile** (`./mvnw spring-boot:run -Dspring-boot.run.profiles=producer`):
  `@Profile("producer")` beans exist only then, and `application-producer.yaml` is loaded on top of
  `application.yaml`. It talks to Kafka only via `KafkaTemplate` with raw JSON, like an external
  sensor. Trade-off: it runs in the service's JVM — fine for a demo, a real feed is separate.
- **Logic is pure, I/O is thin**: `TrackSimulator` (positions) and `DemoScenario` (which messages,
  in which order) are plain classes with unit tests; `DemoProducer` only sends and sleeps.
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
