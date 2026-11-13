# Senior Back-End Engineer Take-Home Assignment

**Track Picture Service (Air Defense Domain)**

## Overview

Build a small back-end system that receives track updates — position reports for airborne objects such as aircraft, UAVs, and missiles — and:

- **Keeps the current picture:** the latest known state of every track, served to a front end through an API.
- **Keeps the history:** every accepted update for a track, queryable over time.

Updates arrive continuously, may be duplicated, and may arrive out of order. The picture must always reflect the most recent valid state, and history must not lose accepted updates. Updates are delivered through a messaging system.

## The Track Update

Each update describes one observation of one track. JSON is fine. It must carry at least:

| Field                  | Type         | Validation                                            |
|------------------------|--------------|-------------------------------------------------------|
| `trackId`              | String       | Required, non-empty                                   |
| `timestamp`            | ISO-8601 UTC | Required                                              |
| `latitude` / `longitude` | Number (deg) | Required; -90..90 / -180..180                       |
| `altitude`             | Number (m)   | Optional; >= 0                                        |
| `heading`              | Number (deg) | Optional; 0 <= heading < 360                          |
| `identity`             | Enum         | Required; one of `UNKNOWN`, `FRIEND`, `NEUTRAL`, `HOSTILE` |

Invalid updates must be rejected and must not reach the picture or the history. Rejections should be visible — at minimum, a log line stating the reason; a dead-letter topic is a stretch goal.

## Architecture

Track updates arrive on a topic named `track-updates`, published by an external source such as sensors or data links that you do not control. Your system consumes it and exposes two read APIs.

- **Picture** — keeps the latest state per `trackId` in memory or persistent storage. Serves `GET /picture` for all tracks and `GET /tracks/{trackId}`.
- **History** — serves `GET /tracks/{trackId}/history`, optionally with `from`/`to` filters. Storage is your choice: SQLite, Postgres, or even an append-only file — justify it.

Whether picture and history are separate processes or modules in one process is up to you.

## Core Requirements

- **Ordering:** an update with an older timestamp than the current state of that track must not overwrite it. Updates for the same track must be processed in order relative to each other; different tracks need not be.
- **Idempotency:** processing the same message twice — redelivery or replay — must not corrupt the picture or duplicate history.
- **Consistency:** picture and history do not need to update at the same moment.
- **A simple producer:** e.g. a small script that publishes updates for a handful of moving tracks to `track-updates`, including at least one duplicate, one out-of-order update, and one invalid message, so your handling is demonstrable.

## Things We're Evaluating

- How you decompose the problem and where you draw boundaries
- Handling of ordering, duplicates, and replay
- Topic and message design choices, and your reasoning for them
- Read API design
- Code clarity, naming, and testability

## Stretch Goals (Optional)

- Route invalid messages to a dead-letter topic instead of only logging them
- Mark tracks stale/dropped when no update arrives for a configurable period
- Rebuild the picture from the topic on restart instead of starting empty
- Unit tests for validation and the ordering/idempotency logic
- Metrics or a health endpoint showing consumer lag

## Deliverables

- Source code in a Git repository — committed to the working branch, no separate worktrees
- A short README covering: how to run it, a simple architecture diagram — ASCII is fine — key design decisions, trade-offs made, and what you'd improve with more time

## Constraints

- Any mainstream back-end language: Java/Kotlin, Go, Python, C#, or Node/TypeScript.
- Time box: ~5–6 hours. We don't expect production-grade code — we want to see how you reason about ordering, consistency, and service boundaries. If something is out of scope, say so in the README rather than silently omitting it.
