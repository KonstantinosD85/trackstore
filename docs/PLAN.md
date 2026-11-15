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
| 5  | Picture storage with "newer timestamp wins" upsert | `@Modifying` queries, `@Transactional` | ⬜ |
| 6  | `TrackUpdateService` + ordering/idempotency tests | Service layer, `@DataJpaTest` | ⬜ |
| 7  | Kafka consumer: JSON parsing, invalid → log + dead-letter topic (incl. tests for unparseable JSON, e.g. `"identity": "ENEMY"`, bad timestamp) | `@KafkaListener`, error handlers | ⬜ |
| 8  | REST API for the three endpoints | `@RestController`, DTOs, 404 handling | ⬜ |
| 9  | Producer (moving tracks + duplicate, out-of-order, invalid messages) | `CommandLineRunner` or script | ⬜ |
| 10 | Stretch: stale tracks, health endpoint / consumer lag | `@Scheduled`, Actuator | ⬜ |
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
- **Flyway is SQL-first**: migrations are hand-written `V<n>__<name>.sql` files, applied once and
  recorded (with a checksum) in `flyway_schema_history`. Never edit an applied migration — add a
  new one. Hibernate runs with `ddl-auto: validate` as a safety net: the app fails at startup if
  entities and tables disagree. (Autogenerate à la Alembic exists via Liquibase + liquibase-hibernate
  or IntelliJ Ultimate, but generated DDL drops named constraints/defaults that matter here.)
- **Duplicates via `INSERT ... ON CONFLICT DO NOTHING`**, not `save()` + catch: a JPA constraint
  violation marks the whole transaction rollback-only, which would also undo the picture update.
- **Validation must mirror DB constraints** (`@Size(max = 64)` ↔ `VARCHAR(64)`); otherwise a message
  passes validation, fails on insert, and becomes a poison message that blocks its partition.
