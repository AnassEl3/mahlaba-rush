# Mahlaba Rush — Project Specification

> A fault-tolerant, event-driven Moroccan *mahlaba* (snack & dairy shop), playable as a game.
> Java · Spring Boot · Apache Kafka · PostgreSQL · Angular · Docker

**Status:** Draft v1
**Purpose:** Learning and showcase project (GitHub + social media)

---

## 1. Vision

Mahlaba Rush is a real event-driven microservices system disguised as a management game.

The player runs a Moroccan mahlaba serving atay, msemen, baghrir, harcha, raib and avocado juice. Customers order, the cashier takes payment, chefs cook, and things go wrong: a street cat knocks over the kitchen, the power flickers, and the ftour rush hits.

**The core rule:** every game mechanic is a real technical operation, and every number on screen is a real metric. Nothing is faked for the animation.

**The core promise:** the architecture guarantees that **no order is ever lost**. The player can't break that guarantee. What the player *can* affect is **speed and customer happiness**. Fault tolerance is the system's job; performance is the player's skill.

## 2. Goals and Non-Goals

### 2.1 Goals

| # | Goal |
|---|------|
| G1 | Learn and demonstrate event-driven architecture (EDA) with Kafka in depth. |
| G2 | Guarantee zero lost orders and zero double-processed orders under every failure scenario in §9. |
| G3 | Make the architecture visible and explainable to non-technical people (customer view) and engineers (X-ray view). |
| G4 | Start the whole system with one command: `docker compose up`. |
| G5 | Produce shareable material: demo GIFs, an end-of-day report screenshot, and a README that explains every design decision. |

### 2.2 Non-Goals

- Authentication and user accounts.
- Real payments: payment is simulated.
- Production deployment and cloud hosting (see §15, Future Work).
- Mobile layout: desktop-first; minimum supported width is 1280 px.
- Multiplayer.

## 3. Domain

### 3.1 Menu

| Item | Price (DH) | Prep time | Ingredients consumed |
|------|-----------:|----------:|----------------------|
| Atay (mint tea) | 8 | 1.0 s | tea, mint, sugar |
| Msemen | 6 | 3.0 s | flour, butter |
| Baghrir | 6 | 3.0 s | semolina, yeast |
| Harcha | 5 | 2.0 s | semolina, butter |
| Raib | 7 | 0.5 s | raib (portion) |
| Jus d'avocat | 15 | 2.0 s | avocado, milk, sugar |

Prep time is real processing time spent inside the kitchen consumer. That's what makes consumer lag build up during the ftour rush.

### 3.2 Characters

These names appear only in the UI and in incident cards. Code stays neutral (`payment-service`, `kitchen-service`).

| Character | Real component |
|-----------|----------------|
| Moul l'mahlaba (the player) | The operator, acting through game-service |
| The cashier | payment-service |
| The chefs | kitchen-service instances |
| The ticket rail | Kafka topic `orders.events` |
| The complaint box | Dead-letter topics |
| Mchicha the street cat | Chaos: kills containers at random |
| The supplier | game-service publishing restock events |

## 4. Architecture

### 4.1 Overview

```
                ┌───────────────┐
 Angular  ◄─SSE─┤  game-service ├──REST──► order-service (customer generator)
 (player) ─REST►│ (BFF, chaos,  │──Docker Engine API──► containers
                │  projections) │◄── consumes all *.events topics
                └───────────────┘

 order-service ──► orders.events ──► payment-service ──► payments.events ──┐
      ▲                │                                                    │
      │                └──────────► kitchen-service ──► kitchen.events ─────┤
      └─────────────────────────────────────────────────────────────────────┘
                     (order-service consumes payments.* and kitchen.*)
```

### 4.2 Principles

1. **Choreography, not orchestration.** There's no central coordinator; each service reacts to events.
2. **order-service is the single owner of order state.** Other services never decide an order's status; they publish facts, and order-service decides.
3. **Database per service.** Each service owns its schema. One Postgres server hosts three logical databases, to save laptop resources.
4. **The game never touches business logic.** The business services don't know the game exists. game-service only observes topics and acts through the same public interfaces a real operator would use (REST, Docker, Kafka).
5. **Every published event goes through a transactional outbox.**
6. **Every consumer is idempotent.**

### 4.3 Services

| Service | Responsibility | Database | Consumes | Produces |
|---------|----------------|----------|----------|----------|
| **order-service** | Accepts orders, owns the order state machine, runs saga timeouts | `orders_db` | `payments.events`, `kitchen.events` | `orders.events` |
| **payment-service** | Simulates charging and refunding | `payments_db` | `orders.events` | `payments.events` |
| **kitchen-service** | Checks and reserves stock, cooks, handles restocks | `kitchen_db` | `orders.events`, `supplies.events` | `kitchen.events` |
| **game-service** | Customer generator, game state projection, score, player actions, chaos, SSE stream, dead-letter replay | none (in memory) | all `*.events` topics | `supplies.events`, chaos injections |
| **frontend** | Angular game UI | — | SSE from game-service | REST to game-service |

### 4.4 Infrastructure

| Component | Spec |
|-----------|------|
| Kafka | 3 nodes, KRaft mode (combined broker + controller), no ZooKeeper |
| Kafka UI | Web UI for inspecting topics, partitions, offsets and consumer groups |
| PostgreSQL | 1 server with `orders_db`, `payments_db`, `kitchen_db` |
| kafka-init | One-shot container that creates topics with the right settings |
| Docker Compose | Single entry point for the whole system |

> **Caveat to document:** KRaft combined mode is fine for a local cluster but not recommended in production, where controllers should run on dedicated nodes.

## 5. Events

### 5.1 Envelope

Every event shares the same envelope:

```json
{
  "eventId": "8f14e45f-ceea-4f6a-9b1c-2c1f3b5e8a77",
  "eventType": "OrderPlaced",
  "eventVersion": 1,
  "occurredAt": "2026-11-10T18:47:03.120Z",
  "orderId": "c2a1...",
  "correlationId": "c2a1...",
  "causationId": "id-of-the-event-that-caused-this-one",
  "producer": "order-service",
  "payload": { }
}
```

| Field | Why it exists |
|-------|---------------|
| `eventId` | Unique ID used for idempotency (deduplication). |
| `eventType` + `eventVersion` | Routing and schema evolution. |
| `occurredAt` | Latency measurement and debugging. |
| `orderId` | Also the Kafka message key, which guarantees per-order ordering inside a partition. |
| `correlationId` | Traces one customer order across all services and logs. |
| `causationId` | Records which event triggered this one, so you can rebuild the causal chain. |

**Schema evolution rule:** consumers ignore unknown fields. A breaking change requires a new `eventVersion`.

### 5.2 Topics

| Topic | Partitions | RF | min ISR | Events |
|-------|-----------:|---:|--------:|--------|
| `orders.events` | 3 | 3 | 2 | OrderPlaced, OrderConfirmed, OrderServed, OrderCancelled |
| `payments.events` | 3 | 3 | 2 | PaymentCompleted, PaymentFailed, PaymentRefunded |
| `kitchen.events` | 3 | 3 | 2 | OrderPrepared, OutOfStock |
| `supplies.events` | 1 | 3 | 2 | IngredientsDelivered |
| `<topic>.DLT` | 3 | 3 | 2 | Dead letters for each topic above |

- `auto.create.topics.enable=false`. Topics are created only by `kafka-init`.
- **Why one topic per domain instead of one per event type:** all lifecycle events for one order live on the same topic with the same key, so they're **strictly ordered**. OrderConfirmed can never arrive after OrderCancelled for the same order.
- **Why 3 partitions:** that allows up to 3 parallel chefs. A 4th chef would sit idle, which is an intentional teaching moment.

### 5.3 Event catalogue

| Event | Producer | Payload (main fields) |
|-------|----------|-----------------------|
| OrderPlaced | order-service | items[], total, customerName |
| OrderConfirmed | order-service | items[] (sent after payment succeeds) |
| OrderServed | order-service | waitTimeMs |
| OrderCancelled | order-service | reason: `PAYMENT_FAILED` · `OUT_OF_STOCK` · `TIMEOUT` |
| PaymentCompleted | payment-service | amount, paymentId |
| PaymentFailed | payment-service | reason |
| PaymentRefunded | payment-service | amount, paymentId |
| OrderPrepared | kitchen-service | chefId (instance ID), prepTimeMs |
| OutOfStock | kitchen-service | missingIngredients[] |
| IngredientsDelivered | game-service | ingredient → quantity |

## 6. Order Saga

### 6.1 State machine (owned by order-service)

```
            PaymentCompleted                 OrderPrepared
 PLACED ─────────────────────► PAID ─────────────────────► SERVED
   │                            │
   │ PaymentFailed / TIMEOUT    │ OutOfStock / TIMEOUT
   ▼                            ▼
 CANCELLED ◄────────────────────┘
```

Terminal states are `SERVED` and `CANCELLED`. Any event that arrives for an order already in a terminal state is ignored and logged.

### 6.2 Flows

**Happy path**
1. `POST /orders` → order saved as `PLACED` + `OrderPlaced` written to the outbox (same transaction).
2. payment-service consumes `OrderPlaced` → charges → `PaymentCompleted`.
3. order-service consumes `PaymentCompleted` → `PAID` → `OrderConfirmed`.
4. kitchen-service consumes `OrderConfirmed` → reserves stock → cooks (prep time) → `OrderPrepared`.
5. order-service consumes `OrderPrepared` → `SERVED` → `OrderServed`.

**Payment failed**
- payment-service emits `PaymentFailed` → order-service sets `CANCELLED (PAYMENT_FAILED)` and emits `OrderCancelled`.
- payment-service receives `OrderCancelled`. Nothing was charged, so it does nothing.

**Out of stock (compensation)**
- kitchen-service emits `OutOfStock` → order-service sets `CANCELLED (OUT_OF_STOCK)` and emits `OrderCancelled`.
- payment-service consumes `OrderCancelled`. A payment was captured, so it refunds and emits `PaymentRefunded`.

**Timeout (customer runs out of patience)**
- An order-service scheduled job cancels every order in `PLACED` or `PAID` older than the patience threshold (default 20 s, configurable per level), setting `CANCELLED (TIMEOUT)` and emitting `OrderCancelled`.
- payment-service refunds if a payment was captured, even if the capture happened *after* the cancellation. Both events are on `orders.events` with the same key, so payment-service always sees `OrderPlaced` before `OrderCancelled`.
- If kitchen-service already started cooking, it finishes and emits `OrderPrepared`. order-service ignores it because the order is already terminal. This is counted as a **wasted dish**: a visible, real consequence of eventual consistency.

### 6.3 Compensation rules

- **One trigger:** every compensation is driven by `OrderCancelled`.
- **Idempotent:** a payment is refunded at most once, enforced by a unique constraint on the refund for each `paymentId`.
- **Order-safe:** compensation never depends on the order in which events arrive *across* topics; it only relies on ordering within `orders.events`.

## 7. Fault Tolerance Requirements

### 7.1 Kafka cluster

| ID | Requirement |
|----|-------------|
| FT-K1 | 3 brokers, replication factor 3, `min.insync.replicas=2` on every topic. |
| FT-K2 | The system keeps accepting and processing orders with 1 broker down. |
| FT-K3 | The KRaft controller quorum (3 voters) survives the loss of 1 node. |

### 7.2 Producers

| ID | Requirement |
|----|-------------|
| FT-P1 | `acks=all`. |
| FT-P2 | `enable.idempotence=true`, so producer retries never create broker-side duplicates. |
| FT-P3 | The message key is `orderId` for every order-related event. |
| FT-P4 | The outbox relay waits for the send to be acknowledged before marking a row as published. |

### 7.3 Transactional outbox

| ID | Requirement |
|----|-------------|
| FT-O1 | Business state change and outbox row are written in the same database transaction. |
| FT-O2 | The relay polls every ~200 ms, using `SELECT … WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`. |
| FT-O3 | The relay sends each row, waits for the acknowledgment, then sets `published_at`. A crash in between means the row is re-sent, so delivery is **at-least-once by design**. |
| FT-O4 | Published rows are purged after a retention period. |

```sql
CREATE TABLE outbox (
  id            BIGSERIAL PRIMARY KEY,
  event_id      UUID        NOT NULL UNIQUE,
  topic         TEXT        NOT NULL,
  message_key   TEXT        NOT NULL,
  event_type    TEXT        NOT NULL,
  payload       JSONB       NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);
```

> **Documented trade-off:** a polling relay is simple and needs no extra infrastructure. Change data capture (Debezium) would be the production-grade upgrade (see §15).

### 7.4 Consumers

| ID | Requirement |
|----|-------------|
| FT-C1 | Offsets are committed only after successful processing. Auto-commit is off, which is the Spring Kafka default. |
| FT-C2 | **Idempotency:** each consumer inserts `(consumer_group, event_id)` into `processed_events` **in the same transaction** as its business change. A duplicate violates the primary key and is skipped. |
| FT-C3 | `auto.offset.reset=earliest`. A brand-new consumer group never skips events. |
| FT-C4 | Each kitchen-service instance runs with listener concurrency 1, so **1 chef = 1 consumer**. |

```sql
CREATE TABLE processed_events (
  consumer_group TEXT        NOT NULL,
  event_id       UUID        NOT NULL,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, event_id)
);
```

> **Interview point:** Kafka's exactly-once transactions only cover Kafka → Kafka processing. Once a database is involved, **outbox + idempotent consumers** is what gives effectively-once processing.

### 7.5 Error handling

| ID | Requirement |
|----|-------------|
| FT-E1 | **Transient errors** (database down, timeouts): blocking retries with exponential backoff (`DefaultErrorHandler`): 4 attempts, 500 ms initial interval, ×2 multiplier, 8 s max. |
| FT-E2 | **Permanent errors** (deserialization, validation): not retried; sent straight to `<topic>.DLT` via `DeadLetterPublishingRecoverer`. |
| FT-E3 | `ErrorHandlingDeserializer` wraps the JSON deserializer, so a poison message never crashes the consumer in a loop. |
| FT-E4 | Dead-letter records keep the original headers (original topic, partition, offset, exception) for diagnosis. |
| FT-E5 | **Replay:** game-service can republish dead-letter records to their original topic. Consumers are idempotent, so replay is safe. |

> **Documented trade-off:** *blocking* retries were chosen over non-blocking retry topics (`@RetryableTopic`), because retry topics break per-order ordering. The cost is that a slow retry delays the rest of its partition. That's acceptable here, because retries are capped.

### 7.6 Service instances

| ID | Requirement |
|----|-------------|
| FT-S1 | payment-service and kitchen-service can run several instances in the same consumer group. |
| FT-S2 | Killing an instance triggers a rebalance; the survivors take over its partitions with no lost events. |
| FT-S3 | Every service exposes `/actuator/health` with liveness and readiness probes. |

### 7.7 Frontend resilience

| ID | Requirement |
|----|-------------|
| FT-F1 | The SSE connection reconnects automatically. On reconnect, the frontend fetches a full snapshot. |
| FT-F2 | The database is the source of truth. The event stream is only for live updates. |

## 8. Game Design

### 8.1 Game ↔ tech mapping

| In the game | What really happens |
|-------------|---------------------|
| A customer orders | game-service calls `POST /orders` on order-service |
| Ticket rail with 3 slots | `orders.events`, 3 partitions |
| Tickets piling up | **Consumer lag** of the `kitchen` group (from Kafka's AdminClient) |
| A chef | A running kitchen-service container |
| **Hire a chef** | Starts another kitchen-service container → rebalance |
| **Let a chef go** | Stops a kitchen-service container → rebalance |
| The 4th chef stands idle | More consumers than partitions |
| Customer patience bar | Saga timeout (§6.2) |
| Customer leaves angry, gets a refund | `OrderCancelled (TIMEOUT)` → `PaymentRefunded` |
| Out of baghrir | `OutOfStock` → compensation |
| **Restock** | game-service publishes `IngredientsDelivered` |
| Complaint box | Dead-letter topics |
| **Handle complaints** | Dead-letter replay (FT-E5) |
| **Restart station** | Starts the stopped container again |
| Mchicha the cat attacks | game-service kills a random service container |
| Power flicker | game-service stops one Kafka broker for N seconds |
| A garbled ticket | game-service publishes malformed bytes to `orders.events` |
| An order shouted twice | game-service republishes an existing event with the same `eventId` |
| Wasted dish | `OrderPrepared` arrives for an order that's already cancelled |
| Ftour rush | The customer generator jumps to a high order rate |

### 8.2 Game loop

A game session is one **service day**:

1. **Opening:** the level script starts (customer rate, scheduled incidents, patience threshold).
2. **Service:** customers arrive; the player reacts with actions; incidents fire.
3. **Closing:** the customer generator stops.
4. **Drain:** wait until every order reaches a terminal state (or 60 s pass).
5. **Reconciliation:** compare the orders accepted by order-service (201 responses) with orders in a terminal state. **Lost orders must be 0.**
6. **End-of-day report.**

### 8.3 Metrics and score

| Metric | Source |
|--------|--------|
| Served / Cancelled / Refunded | Counts of terminal events |
| **Lost orders** | Reconciliation (§8.2, step 5) |
| Average and p95 wait time | `OrderServed.occurredAt − OrderPlaced.occurredAt` |
| Revenue (DH) | Sum of served orders, minus wasted-dish ingredient cost |
| Consumer lag per partition | AdminClient, polled every 1 s |
| Duplicates caught | Idempotency rejections, exposed as a metric by each service |
| Complaints (dead-letter count) | End offsets of the dead-letter topics |
| Wasted dishes | `OrderPrepared` for an order that's already terminal |

**Happiness (0–100), starts at 70:**
- Served in under 5 s: +2
- Served in under 10 s: +1
- Timeout: −8
- Refund (out of stock): −4
- Payment failed: −2

**Game over:** happiness reaches 0.
**Star rating (1–5):** based on final happiness, p95 wait time and revenue.

### 8.4 Player actions

| Action | Cost | Effect | Cooldown |
|--------|------|--------|----------|
| Hire chef | 20 DH | Starts kitchen-N (max 4) | 3 s |
| Let chef go | — | Stops kitchen-N (min 1) | 3 s |
| Hire cashier | 15 DH | Starts payment-N (max 2) | 3 s |
| Restock | 30 DH | Publishes `IngredientsDelivered` | 5 s |
| Handle complaints | — | Replays dead letters | 5 s |
| Restart station | — | Starts a crashed container | — |

### 8.5 Levels

Each level teaches **one** concept and ends with a lesson card.

| # | Level | Script | Concept taught |
|---|-------|--------|----------------|
| 1 | **Opening Day** | Low, steady customer rate. No incidents. | What an event is; producer, topic, consumer |
| 2 | **The Cat** | Mchicha kills kitchen-service twice. | Offsets, at-least-once delivery, catching up after a restart |
| 3 | **Power Flicker** | One broker goes down for 30 s. | Replication, in-sync replicas, `acks=all` |
| 4 | **Ftour Rush** | Customer rate ×10 for 2 minutes. | Partitions, consumer groups, scaling, the 4th-chef trap |
| 5 | **Bad Tickets** | Malformed orders + duplicates + a payment-service crash. | Dead-letter topics, poison messages, idempotency |
| ∞ | **Free Play** | Random incidents. The player can trigger chaos manually. | Everything |

### 8.6 Lesson card format

Every incident and every level end has a card with two lines:

- **Story:** *"Mchicha knocked over the kitchen!"*
- **Tech:** *"kitchen-service crashed. Its offsets weren't committed, so the tickets stay on the rail. Restart it and it continues exactly where it stopped."*

## 9. Failure Scenarios (Acceptance Tests)

Every scenario is an automated integration test **and** a playable game event.

| # | Scenario | Expected result |
|---|----------|-----------------|
| F1 | Kill 1 of 3 brokers during traffic | 0 lost, processing continues |
| F2 | Kill kitchen-service, place 20 orders, restart it | All 20 processed after restart |
| F3 | Kill 1 of 2 payment-service instances | Rebalance; all orders processed |
| F4 | The same event delivered twice | Processed once; duplicate counter +1 |
| F5 | Malformed message on `orders.events` | Lands in `orders.events.DLT`; other messages unaffected |
| F6 | Postgres down for 15 s | Retries with backoff; everything processed after recovery |
| F7 | order-service crashes between the database commit and publishing to Kafka | The outbox relay publishes after restart |
| F8 | payment-service down longer than the patience threshold | Order cancelled (TIMEOUT); refunded once payment-service comes back and charges |
| F9 | Out of stock | Order cancelled, payment refunded exactly once |
| F10 | Replay the dead-letter topic twice | Each record has its effect at most once |

**Universal invariant:** after the drain phase, every order accepted with a 201 is in a terminal state, and every captured payment of a cancelled order has exactly one refund.

## 10. game-service

### 10.1 Responsibilities

- **Customer generator:** calls `POST /orders` at the rate defined by the current level script.
- **Projection:** consumes all `*.events` topics with its own consumer group (`game-observer`) and keeps the current day's game state in memory.
- **Metrics:** computes lag (AdminClient), wait times, score and happiness.
- **Player actions:** exposed over REST, executed via the Docker Engine API, Kafka or REST.
- **Chaos:** kills or stops containers and brokers, injects malformed messages and duplicates.
- **Streaming:** SSE to the frontend.

### 10.2 API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/game/stream` | SSE: game snapshot every 250 ms, plus incident events |
| GET | `/game/snapshot` | Full current state (used on reconnect) |
| POST | `/game/days` | Start a day `{ "level": 1..5 \| "free" }` |
| POST | `/game/actions/chefs` | Hire a chef |
| DELETE | `/game/actions/chefs` | Let a chef go |
| POST | `/game/actions/cashiers` | Hire a cashier |
| POST | `/game/actions/restock` | Restock ingredients |
| POST | `/game/actions/complaints/replay` | Replay the dead-letter topics |
| POST | `/game/actions/restart/{service}` | Restart a crashed container |
| POST | `/game/chaos/{type}` | `cat` · `power-flicker` · `bad-ticket` · `duplicate` · `rush` |
| GET | `/game/events/{orderId}` | Full event history of one order (for the event inspector) |

### 10.3 Container control

- game-service manages a **fixed pool of containers declared in Compose** (`kitchen-1..4`, `payment-1..2`). Extra instances start in the `stopped` state.
- "Hire" means starting a stopped container from the pool; "kill" means stopping a running one. No containers are created dynamically.
- Uses the Docker Engine API through the mounted Docker socket.

> **Security caveat to document:** mounting `/var/run/docker.sock` gives a container root-equivalent control over the host. That's acceptable for a local demo and never acceptable in production.

### 10.4 Efficiency

- Never forward individual events to the browser at high throughput. Send **aggregated snapshots every 250 ms**.
- Poll lag once per second, not once per event.
- The snapshot contains only the latest 10 tickets per rail, plus counts.

## 11. Frontend

### 11.1 Tech

- Latest Angular with standalone components, **signals**, and `OnPush` change detection.
- The scene is built with SVG + CSS transforms for animation; no game engine.
- SSE via `EventSource`, wrapped in a service with automatic reconnection and a snapshot fetch on reconnect.

### 11.2 Screens

| Screen | Content |
|--------|---------|
| **Home** | Title, level selection (locked/unlocked), free play |
| **Shop (main)** | The scene, top bar, action bar, incident cards, event inspector |
| **End-of-day report** | Stats, star rating, lesson recap, "share" layout ready for screenshots |
| **About / How it works** | The architecture diagram and the game ↔ tech mapping table |

### 11.3 Shop layout

```
┌───────────────────────────────────────────────────────────────┐
│ MAHLABA RUSH   Level 4 • Ftour in 02:13          💰 1,240 DH  │
│ 😊 ████████░░  Served 87 • Lost 0 • Refunds 3 • Wasted 1      │
├───────────────────────────────────────────────────────────────┤
│  👥👥👥 → [Cashier] → 🎫 TICKET RAIL 🎫 → [Kitchen] → 🛎 Served │
│  queue +    💳          ▮▮▮ │ ▮ │ ▮▮      👨‍🍳 👩‍🍳 👨‍🍳 🧍           │
│  patience               rail 1  2  3     chefs (4th idle)     │
│                         📮 Complaints (2)          💡💡⚫ power │
├───────────────────────────────────────────────────────────────┤
│ [+Chef] [-Chef] [+Cashier] [Restock] [Complaints] [🔍 X-ray]  │
└───────────────────────────────────────────────────────────────┘
```

### 11.4 Two views

- **Customer view (default):** an illustrated mahlaba with zellige tiles, a warm palette, a steaming teapot, customers with patience bars, and chefs at the griddle. Understandable by anyone.
- **X-ray view (toggle):** the same scene with real technical labels overlaid:
  - the rail shows topic name, partition numbers, offsets and lag per partition
  - chefs show container name, consumer ID and assigned partitions
  - the power lamps show broker IDs and status, plus the in-sync replica count per topic
  - the complaint box shows dead-letter topic names and record counts
  - clicking any ticket opens the **event inspector**: the raw event JSON, key, partition, offset and the order's full event history

### 11.5 UI rules

- Show at most 10 visible tickets per rail, plus a "+N" badge.
- Animate only with `transform` and `opacity`.
- Never use color as the only signal: always pair it with an icon or text.
- Incident cards auto-dismiss after 8 s and can be pinned.

## 12. Observability

| Requirement | Implementation |
|-------------|----------------|
| Structured logs | JSON logs including `correlationId`, `eventId` and `eventType` |
| Health | Spring Boot Actuator on every service |
| Kafka inspection | Kafka UI |
| Custom metrics | Idempotency rejections, outbox backlog size, dead-letter count, via Micrometer |
| *(stretch)* Dashboards | Prometheus + Grafana |
| *(stretch)* Tracing | OpenTelemetry, with trace context propagated through Kafka headers |

## 13. Tech Stack and Repository

### 13.1 Stack

| Layer | Choice |
|-------|--------|
| Language | Java 21+ (LTS) |
| Backend | Spring Boot (latest stable), Spring for Apache Kafka, Spring Data JPA |
| Migrations | Flyway |
| Messaging | Apache Kafka 4.x, KRaft mode |
| Database | PostgreSQL 16+ |
| Frontend | Angular (latest), TypeScript |
| Build | Maven multi-module |
| Tests | JUnit 5, Testcontainers (Kafka, Postgres), Awaitility |
| Runtime | Docker, Docker Compose |

### 13.2 Repository layout

```
mahlaba-rush/
├── README.md
├── SPEC.md
├── docs/
│   ├── architecture.md          # diagrams + explanations
│   ├── adr/                     # architecture decision records
│   └── lessons/                 # one page per concept, linked from the game
├── docker-compose.yml
├── infra/
│   ├── kafka-init/              # topic creation script
│   └── postgres-init/           # creates the 3 databases
├── backend/
│   ├── pom.xml                  # parent POM
│   ├── mahlaba-events/          # shared event envelope + payload records
│   ├── mahlaba-messaging/       # shared outbox, idempotency, error-handling config
│   ├── order-service/
│   ├── payment-service/
│   ├── kitchen-service/
│   └── game-service/
└── frontend/
    └── mahlaba-ui/
```

### 13.3 Shared modules

- **mahlaba-events:** event envelope and payloads as Java records. *Trade-off to document in an ADR:* a shared module couples the services at build time. The alternative is a schema registry with JSON Schema or Avro, which is a stretch goal.
- **mahlaba-messaging:** the reusable outbox relay, idempotent-consumer support and the standard error handler. Every service gets identical reliability behavior.

## 14. Architecture Decision Records (to write)

| ADR | Decision |
|-----|----------|
| 001 | Choreography over orchestration |
| 002 | order-service as single owner of order state |
| 003 | One topic per domain, keyed by `orderId` |
| 004 | Transactional outbox with polling relay (vs CDC/Debezium) |
| 005 | Idempotent consumers via `processed_events` table |
| 006 | Blocking retries over retry topics (ordering preserved) |
| 007 | Shared events module (vs schema registry) |
| 008 | One Postgres server with logical database-per-service |
| 009 | game-service as an external operator (Docker socket caveat) |
| 010 | Aggregated SSE snapshots instead of per-event streaming |

## 15. Future Work

- Change data capture with Debezium instead of the polling outbox relay.
- Schema Registry with Avro or JSON Schema, plus compatibility checks.
- Orchestrated saga variant for comparison.
- Delivery service (a new station in the game).
- Prometheus + Grafana dashboards, OpenTelemetry tracing.
- Kubernetes deployment, with pod kills as chaos.
- Hosted public demo with per-visitor sandboxed sessions.

## 16. Milestones

| Milestone | Outcome |
|-----------|---------|
| **M0 — Foundations** | Repo, 3-broker Kafka cluster, Postgres, Kafka UI, topics, all running via Compose. |
| **M1 — Happy path** | order → payment → kitchen → served, with outbox and idempotent consumers. |
| **M2 — Saga** | Payment failure, out of stock, timeouts, refunds, wasted dishes. |
| **M3 — Reliability** | Retries, dead-letter topics, poison messages, replay, multiple instances. |
| **M4 — Failure tests** | Scenarios F1–F10 automated with Testcontainers. |
| **M5 — game-service** | Generator, projection, metrics, actions, chaos, SSE. |
| **M6 — Frontend** | Customer view, X-ray view, actions, incidents, report. |
| **M7 — Levels** | Scripted levels 1–5 and lesson cards. |
| **M8 — Launch** | README, ADRs, diagrams, demo GIFs, posts. |

**MVP = M0–M6 + Level 4 (Ftour Rush) + the cat.** Everything else is a bonus.

## 17. Definition of Done

- [ ] `docker compose up` starts everything on a clean machine.
- [ ] Scenarios F1–F10 pass as automated tests.
- [ ] The universal invariant (§9) holds after every scripted level.
- [ ] The X-ray view shows real topic, partition, offset, lag and broker data.
- [ ] The README explains the architecture at three levels: anyone, developer, interviewer.
- [ ] Every ADR in §14 is written.
- [ ] A demo GIF shows: an order flowing, the cat attack and recovery, the ftour rush, and the X-ray toggle.

## 18. Glossary

| Term | Plain explanation | Mahlaba analogy |
|------|-------------------|-----------------|
| Event | A fact that happened, published for anyone interested | A ticket pinned on the rail |
| Topic | A named, append-only log of events | The ticket rail |
| Partition | A slice of a topic; ordering is guaranteed within a partition | One slot on the rail |
| Offset | A consumer's position in a partition | A chef's bookmark: "I'm at ticket 42" |
| Consumer group | Instances sharing the work of reading a topic | The kitchen team |
| Rebalance | Partitions redistributed when instances join or leave | Chefs swapping slots when one leaves |
| Consumer lag | Events published but not yet processed | Tickets waiting on the rail |
| Replication / in-sync replica | Copies of each partition kept on several brokers | Photocopies of each ticket in 3 drawers |
| Outbox | Events stored in the database with the business change, published afterwards | Writing the ticket in the notebook before pinning it |
| Idempotency | Processing the same event twice has the effect of processing it once | Ignoring an order shouted twice |
| Saga | A multi-step business process made of local transactions plus compensations | Order, pay, cook, or refund if something fails |
| Compensation | An action that undoes an earlier step | Refunding the customer |
| Dead-letter topic | Where events that can't be processed are parked for inspection | The complaint box |
| Eventual consistency | Services agree on the state, but not instantly | The kitchen hears about a cancellation a moment after the cashier |
