---
name: add-event-consumer
description: Add or change a Kafka event, producer, or consumer in a Mahlaba Rush backend service while keeping the fault-tolerance guarantees (outbox, idempotency, error handling). Use whenever code publishes to or listens on a Kafka topic.
---

# Add an event or consumer

The project's core promise is **zero lost and zero double-processed orders** (SPEC §2, §9).
Every Kafka change must preserve it. Source of truth: `assets/doc/SPEC.md` §5–§7.

## Adding / changing an event

1. Add the payload as a Java record in `backend/mahlaba-events/`; it travels inside the shared
   envelope (§5.1) — never invent a new top-level shape.
2. Add it to the event catalogue table in SPEC §5.3 and pick its topic from §5.2.
   Never create topics from code: `auto.create.topics.enable=false`; add them to `infra/kafka-init/`.
3. Breaking payload change → bump `eventVersion`. Consumers must ignore unknown fields.

## Publishing

- Never call `KafkaTemplate` from business code. Write an `outbox` row **in the same
  transaction** as the state change (FT-O1); the relay in `mahlaba-messaging` publishes it.
- Key = `orderId` for every order-related event (FT-P3).
- Set `causationId` to the `eventId` of the event being handled, and carry `correlationId` through.

## Consuming

Checklist — every item is required:

- [ ] Insert `(consumer_group, event_id)` into `processed_events` in the same transaction as the
      business change; a PK violation means duplicate → skip and increment the duplicates metric (FT-C2).
- [ ] Offsets committed only after success; no auto-commit (FT-C1). `auto.offset.reset=earliest` (FT-C3).
- [ ] Use the shared error handler from `mahlaba-messaging`: blocking retries for transient errors,
      straight to `<topic>.DLT` for deserialization/validation errors (FT-E1–E4). No `@RetryableTopic`.
- [ ] Only order-service changes order status. Other services publish facts (§4.2).
- [ ] Events for an order already `SERVED`/`CANCELLED` are ignored and logged (§6.1).
- [ ] Compensation is triggered only by `OrderCancelled` and is idempotent (§6.3).

## Verify

Add or extend a Testcontainers integration test (Kafka + Postgres, Awaitility) that covers the
happy path and at least the matching failure scenario from SPEC §9 (F1–F10). At minimum, deliver
the same event twice and assert it is processed once (F4).
