---
title: "Building a Reactive Data Plane in TypeScript: Lessons from EventHorizon"
description: "A deep dive into building an event-driven telemetry pipeline with RabbitMQ, MongoDB, and WebSockets — covering at-least-once delivery, idempotent receivers, change stream recovery, and graceful shutdown."
date: 2026-04-02
tags: ["typescript", "distributed-systems", "rabbitmq", "mongodb", "event-driven"]
draft: false
---

# Building a Reactive Data Plane in TypeScript: Lessons from EventHorizon

I've been building a project called EventHorizon — a TypeScript/Node.js event-driven telemetry pipeline. The goal isn't a useful product; the goal is the plumbing. It's a deliberate practice environment for distributed systems patterns with real infrastructure: RabbitMQ, MongoDB, WebSockets, change streams.

If you're newer to message-driven backends, the patterns here should give you names for problems you'll eventually hit. If you've run these systems in production, some of this will be familiar — the interesting parts are in the details.

Here's what I built and what pushed back.

---

## The Architecture: Four Named Planes

The system is split into four planes that data flows through in one direction only. Nothing flows backwards.

```
Ingestion → Processing → Storage → Observation
```

| Plane | What it does |
|---|---|
| **Ingestion** | HTTP entry point, Zod validation, publishes to RabbitMQ |
| **Processing** | AMQP consumer, enriches and classifies events, acks/nacks |
| **Storage** | Append-only MongoDB writes, idempotent inserts |
| **Observation** | MongoDB change stream → WebSocket push, live metrics |

Naming the planes upfront was one of the better decisions I made. When a bug appears, you immediately know which plane owns it. When you add a feature, you know exactly which file it belongs in. "Where does queue depth monitoring go?" is not a question you need to ask — it's Observation, obviously.

---

## Fail-Fast at Every Boundary

The first file I wrote was `config.ts`. It validates every environment variable with Zod before anything else runs. If `MONGO_URI` is missing, the process exits immediately with a field-level error message.

The alternative is the pattern everyone has written at some point:

```ts
// ❌ The naive version
const prefetch = Number(process.env.WORKER_PREFETCH) || 5;
```

This silently coerces `"not-a-number"` to `NaN`, which is falsy, so you get the default with no warning. You spend 20 minutes wondering why your worker is acting strange, then realize the env var was never set.

The same principle applies later in the worker startup sequence. The worker connects to **MongoDB before RabbitMQ**. This is intentional. If you connect to RabbitMQ first and MongoDB is unreachable, the worker starts consuming messages it cannot persist. You ack them. They're gone forever. By connecting to MongoDB first, a failed startup leaves messages safely in the broker queue — the broker holds them until a healthy worker comes up.

---

## Schema as the Single Source of Truth

Every event type in the system is defined once, as a Zod discriminated union, in `src/ingestion/event.schema.ts`. All four planes import from there.

```ts
// ✅ Type derived from schema — can never drift
const SensorSchema = z.object({ sensorId: z.string(), value: z.number() });
type SensorEvent = z.infer<typeof SensorSchema>;
```

I never write a TypeScript interface that duplicates a Zod schema. The type IS the schema. Once I internalized this rule, an entire class of bugs became impossible: the runtime shape and the compile-time type are always in sync.

The discriminated union also means TypeScript narrows correctly when you branch on `event.type`. No `as` casts. No `any`. The compiler knows what fields exist on a sensor event versus a pipeline event.

---

## At-Least-Once Delivery + Idempotent Receiver

This is the core reliability guarantee of the pipeline — and the pairing that catches most people the first time they build a message consumer.

In RabbitMQ (and Kafka, and most brokers), a consumer *acknowledges* a message to tell the broker it was handled successfully. The question is: when do you ack?

**At-least-once delivery** means acking only *after* the work is done — in this case, after writing to MongoDB. If the worker crashes between the write and the ack, the broker redelivers the message to another consumer. The message is never lost, but it may be processed more than once.

That "more than once" part is where newcomers hit a wall. The fix is an **idempotent receiver** — a consumer that produces the same result whether it processes a message once or a hundred times. Here, the MongoDB `events` collection has a unique index on `{ "raw.id": 1 }`. A duplicate insert throws error code 11000, which the repository catches and silently ignores. From the caller's perspective, writing an event you've already stored is a no-op.

The detail that matters: the repository catches *only* 11000, nothing else.

```ts
} catch (err) {
  if (err instanceof MongoServerError && err.code === 11000) return; // known-safe duplicate
  throw err; // real failures re-throw and engage retry logic
}
```

If you catch all `MongoServerError` types, a disk-full error or auth failure looks identical to a safe duplicate — the message gets acked and permanently lost. Narrow exception handling is load-bearing here.

---

## Competing Consumers + Prefetch

The worker is designed to scale horizontally. Start more worker processes, and RabbitMQ distributes messages across all of them in round-robin. No coordination code. No shared state. The broker handles it.

But without one setting, this breaks completely: `channel.prefetch(N)`.

Without prefetch, the broker pushes the entire queue to the first consumer that connects. If there are 50,000 messages in the queue, all 50,000 land in that consumer's memory. The second worker gets nothing. The first worker OOMs or backs up behind a single slow message (head-of-line blocking).

`channel.prefetch(5)` tells the broker: deliver at most 5 unacknowledged messages to this consumer at a time. Only after the worker acks one does the broker deliver the next. Workers that ack faster naturally receive more messages — throughput scales with capacity.

---

## Dead-Letter Exchange for Poison Messages

Failed messages don't get requeued to the front of the queue. That's `nack(msg, false, true)` — requeue=true — and it's a trap. A poison message that always fails blocks every message behind it indefinitely. All consumers see it first, all consumers fail it, it goes straight back to the front.

Instead, the worker republishes failed messages to the *back* of the queue with an incremented `x-retry-count` header. After three retries, it's routed to a dead-letter exchange (`events.dlx`) → `events.dead` queue. From there it can be inspected and replayed manually.

The topology is declared once in `queue.ts` and is idempotent — safe to call on every startup. If anything already exists with matching arguments, `assertExchange()` / `assertQueue()` are no-ops.

---

## Change Stream Resume Token Recovery

The Observation plane watches MongoDB for new inserts using a change stream. The change stream emits an event every time a document is inserted into the `events` collection, which gets pushed over WebSocket to connected dashboard clients.

The problem: change streams die. MongoDB restarts, replica set elections happen, network blips occur. A naive implementation either crashes the server or silently stops delivering events.

The solution is resume token recovery with exponential backoff. MongoDB returns a resume token with every change event. On cursor error, the stream reopens using the last seen token — MongoDB replays any events missed during the outage from the oplog. The retry delay starts at 1 second and doubles to a maximum of 30 seconds, resetting on successful delivery.

This is exactly how production change stream consumers work. The resume token is the checkpoint.

---

## The Hardest Part: Graceful Shutdown Order

Getting shutdown right took longer than any other single thing. The order matters:

1. Stop accepting HTTP requests (Fastify stop)
2. Cancel the AMQP consumer (stop pulling new messages)
3. Finish the in-flight message (let the current handler complete)
4. Close the change stream
5. Close MongoDB
6. Close AMQP channel, then connection
7. `process.exit(0)`

If you close MongoDB before the in-flight message finishes, the write fails and you double-deliver. If you close the AMQP connection before the channel, you lose the ack for the in-flight message. If you stop the change stream before MongoDB closes, you get uncatchable errors.

The order isn't arbitrary — each step is a dependency of the ones before it.

---

## Things That Bit Me

**`process` not found — in a Node.js project.** First compile of `config.ts`, first error. TypeScript 6 with `module: "NodeNext"` treats any file with imports as an ES module, and `@types/node` only surfaces its globals in ambient (non-module) files. The tempting fix — add `"dom"` to the `lib` array — works, but now `window` and `document` typecheck silently in server code. The actual fix is a single ambient file (`global.d.ts`) with `/// <reference types="node" />` and no imports. One line, surgical, doesn't drag in browser types.

**Zod 4 validates UUID version nibbles. Zod 3 didn't.** Test fixtures have a long tradition of using sequential fake IDs like `00000000-0000-0000-0000-000000000001`. Zod 4 enforces RFC 4122 — the version nibble (that first character of the third group) must be `1–8`. Version `0` fails. The fix is using a real v4 UUID in fixtures, which is probably the right call anyway.

**Top-level `await` makes modules untestable.** An early draft connected to RabbitMQ at the module top level — `const channel = await amqp.connect(...)` outside any function. That connection attempt runs on import, before `vi.mock()` can intercept anything. Every test file that imports the module tries to reach a real broker. The fix is straightforward — wrap connection logic in an exported function — but it's the kind of thing that only stings once before you stop doing it.

---

## Why This Stack, Why These Patterns

These aren't toy patterns. They transfer directly:

- **Kafka consumers** — same prefetch/backpressure tradeoffs, same at-least-once + idempotent receiver pairing
- **Event sourcing** — change streams and resume tokens are exactly how you rebuild read models from the write log
- **Any message-driven system** — competing consumers, dead-letter queues, and poison message detection are universal

The stack is RabbitMQ + MongoDB rather than Kafka + Postgres, but the thinking is the same. Distributed systems have a relatively small set of failure modes; what changes is the vocabulary for describing them and the specific primitives each platform exposes.

Reps on one stack sharpen the instincts you bring to the next one.

---

*Stack: TypeScript 6 (strict, NodeNext ESM), Fastify 5, RabbitMQ via amqplib, MongoDB 7, Vitest, raw WebSockets via @fastify/websocket. No ORMs, no frameworks above Fastify, no socket.io.*
