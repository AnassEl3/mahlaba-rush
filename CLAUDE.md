# Mahlaba Rush

Event-driven microservices system (a Moroccan snack & dairy shop) disguised as a management game.
Learning + showcase project. **Full spec: `assets/doc/SPEC.md` — read the relevant section before
designing anything.**

## Status

Greenfield: only the spec exists. Follow the milestones in SPEC §16 in order (M0 Foundations first).
MVP = M0–M6 + Level 4 (Ftour Rush) + the cat.

## Stack

Java 21 · Spring Boot · Spring for Apache Kafka · Spring Data JPA · Flyway · Kafka 4.x (KRaft, 3 nodes)
· PostgreSQL 16 · Angular · Maven multi-module · JUnit 5 + Testcontainers + Awaitility · Docker Compose.

## Repository layout (target, SPEC §13.2)

```
docker-compose.yml        # `docker compose up` must start everything
infra/kafka-init/         # the ONLY place topics are created
infra/postgres-init/      # creates orders_db, payments_db, kitchen_db
backend/                  # Maven parent POM
  mahlaba-events/         # shared envelope + payload records
  mahlaba-messaging/      # outbox relay, idempotent consumer, error handler
  order-service/ payment-service/ kitchen-service/ game-service/
frontend/mahlaba-ui/      # Angular
docs/architecture.md, docs/adr/, docs/lessons/
```

## Non-negotiable rules

These guarantee "no order is ever lost". Don't trade them away for convenience.

1. **Choreography, not orchestration.** No central coordinator.
2. **Only order-service decides order status.** Others publish facts.
3. **Database per service.** Never read another service's schema.
4. **The game never touches business logic.** game-service acts only via REST, Docker, Kafka.
5. **Every published event goes through the transactional outbox.** No direct `KafkaTemplate.send` from business code.
6. **Every consumer is idempotent** via `processed_events`, in the same transaction as the change.
7. Message key = `orderId`. Producers use `acks=all`, idempotence on. Topics: RF 3, min ISR 2.
8. Blocking retries + DLT, not `@RetryableTopic` (it breaks per-order ordering).

Use the `add-event-consumer` skill for any Kafka producer/consumer work and `write-adr` for design decisions.

## Commands

Not set up yet. Expected once M0 lands:

```bash
docker compose up -d          # whole system
mvn -f backend/pom.xml verify # backend build + Testcontainers tests
npm --prefix frontend/mahlaba-ui run start
```

Update this section when the real commands exist.

## Conventions

- Every feature that touches messaging needs an integration test mapped to a SPEC §9 scenario (F1–F10).
- Documented trade-offs in the SPEC become ADRs (SPEC §14); don't decide them silently in code.
- Game text uses the two-line lesson card format: **Story** + **Tech** (SPEC §8.6).
- Desktop-first UI, min width 1280 px. No auth, no real payments.
