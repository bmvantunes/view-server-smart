# View Server Context

This context defines the language for the View Server project: a type-safe live view system that serves initial snapshots and live deltas from an authoritative in-memory engine to React applications over a real server or an in-memory test runtime.

## Language

### Product Concepts

**View Server**:
A runtime that owns configured live topics, ingests row mutations, evaluates live queries, and streams snapshots, deltas, and health to clients.
_Avoid_: Database wrapper, cache server, query proxy

**Real View Server**:
A deployed View Server runtime that serves browser clients over Effect RPC WebSocket and receives mutations from server-side sources.
_Avoid_: Production provider, remote mock, websocket provider

**In-Memory View Server**:
A View Server runtime created inside the current process for tests, demos, Storybook, and browser benchmarks. It uses the same Runtime Core as the Real View Server and swaps only the transport Adapter.
_Avoid_: Mock server, fake client, test hook

**View Server Topic**:
A configured logical table with one schema, one row key field, and one authoritative store.
_Avoid_: Kafka topic, channel, collection

**Topic Row**:
A schema-decoded object stored in a View Server Topic.
_Avoid_: Record, document, message

**Row Key**:
The configured string field that uniquely identifies a Topic Row and acts as the final deterministic sort tiebreaker.
_Avoid_: Primary key when discussing external databases, id unless the configured field is actually named id

### Query Concepts

**Live Query**:
A typed query against one View Server Topic that returns an initial Snapshot and then Deltas for the same result window.
_Avoid_: Subscription query, watch, listener

**Raw Query**:
A Live Query that selects explicit row fields, optionally filters, sorts, offsets, and limits rows.
_Avoid_: Select-all query, table scan

**Grouped Query**:
A Live Query that groups rows by explicit fields and returns aggregate aliases.
_Avoid_: Aggregate-only query, report query

**Snapshot**:
The first event for a Live Query, containing the current result rows, keys, totalRows, and version.
_Avoid_: Initial response, full refresh

**Delta**:
A live event describing inserts, updates, moves, or removals needed to advance a Snapshot result from one version to another.
_Avoid_: Patch when referring to client-visible result changes

**Status Event**:
A live event describing readiness, staleness, closure, backpressure, or typed query/runtime failure for a Live Query.
_Avoid_: Error string, log message

**Subscription**:
The server-side lifetime of one Live Query, including its event stream and close/finalizer behavior.
_Avoid_: WebSocket connection, React hook

### Engine Concepts

**Column Live View Engine**:
The authoritative in-memory engine that owns topics, validates rows, evaluates queries, creates snapshots, computes deltas, tracks subscriptions, and reports engine health.
_Avoid_: chDB replacement, database adapter, query helper

**Columnar Topic Store**:
The per-topic storage and mutation unit behind a View Server Topic; it is expected to become column-oriented as performance work deepens.
_Avoid_: Map wrapper, row array, topic state bag

**Topic Column Vector**:
The schema-derived per-field storage inside a Columnar Topic Store. A Topic Column Vector may use a specialized representation such as a numeric typed array or a generic object array, but callers interact through the Columnar Topic Store.
_Avoid_: Public column API, typed-array contract

**Active Query**:
The engine-side representation of a compiled Live Query that can evaluate snapshots and deltas and may be shared by equivalent subscriptions.
_Avoid_: Query object, filter function

**Raw Query Plan**:
The compiled internal representation of a Raw Query, including predicate hints, deterministic ordering, projection, cache keys, and window scan inputs.
_Avoid_: Query object, filter callback, storage scan object

**Raw Predicate Plan**:
The storage-admissible predicate hint set compiled from a Raw Query, including exact scalar filters and whether row callback evaluation is still required.
_Avoid_: Filter helper, where object, matcher callback

**Raw Ordered Window Index**:
The per-topic ordered slot index used to seek bounded Raw Query windows by storage order and predicate range/equality hints.
_Avoid_: Sort cache, ordered array helper, top-k shortcut

**Grouped Query Plan**:
The compiled internal representation of a Grouped Query, including group key calculation, aggregate definitions, ordering, window settings, and cache keys.
_Avoid_: Grouped query object, aggregate config, groupBy helper

**Health Ledger**:
The owner of counters and sampled health state for mutations, subscriptions, queues, backpressure, ingestion, and transport pressure.
_Avoid_: Health object builder, metrics dump

**Runtime Core**:
The shared engine-backed runtime Module that owns the Column Live View Engine instance, Runtime Client, Live Client, pushed health streams, and lifecycle. Real and in-memory View Servers use the same Runtime Core; only transport and ingress Adapters differ.
_Avoid_: In-memory implementation, test runtime, WebSocket server

### Client And Transport Concepts

**Live Client**:
The transport-neutral client interface consumed by React and in-memory adapters to subscribe to Live Queries and read client-side health.
_Avoid_: Remote client, browser client when the transport is not relevant

**Runtime Client**:
The server-side or in-memory mutation interface used to publish, patch, delete, snapshot, reset, and read fresh runtime health.
_Avoid_: Browser client, live client

**Remote Browser Client**:
The read-only browser client adapter that talks to the Real View Server over the Wire Protocol.
_Avoid_: Runtime client, publishing client

**Wire Protocol**:
The Effect RPC WebSocket protocol using NDJSON serialization and schema-aware JSON-safe encoding for configured topic rows and query values.
_Avoid_: Raw WebSocket protocol, HTTP stream, SSE, MessagePack protocol

**Field Filter Codec**:
The Wire Protocol module that encodes and decodes schema-aware JSON-safe Raw Query filter values, including operator filters and structured-value fallback.
_Avoid_: Filter helper, JSON helper, where encoder

**Raw Query Codec**:
The Wire Protocol module that validates, encodes, and decodes Raw Query wire payloads while preserving configured Topic Row field semantics.
_Avoid_: Raw query helper, select validator, query parser

**Grouped Query Codec**:
The Wire Protocol module that validates, encodes, and decodes Grouped Query wire payloads, including aggregate alias safety, grouped ordering, and numeric aggregate rules.
_Avoid_: Aggregate helper, groupBy validator, grouped query parser

**Aggregate Row Codec**:
The Wire Protocol module that encodes and decodes grouped aggregate row values without precision loss, including bigint and BigDecimal aggregate envelopes.
_Avoid_: Number helper, aggregate JSON helper, sum formatter

**Health Summary Codec**:
The Wire Protocol module that validates, encodes, and decodes the compact pushed health summary stream.
_Avoid_: Health helper, summary JSON helper, status formatter

**Health Topic Codec**:
The Wire Protocol module that validates, encodes, and decodes the pushed per-topic health stream.
_Avoid_: Topic health helper, health row parser, metrics formatter

**Health Payload Codec**:
The Wire Protocol module that validates full runtime health payloads against configured View Server Topics.
_Avoid_: Health object checker, runtime health helper, admin health parser

**View Server Provider**:
The React provider that supplies a Live Client to hooks.
_Avoid_: Runtime provider, in-memory provider when discussing the generic provider

**View Server In-Memory Provider**:
The React testing provider that owns an In-Memory View Server and supplies its Live Client to the same hooks used in production.
_Avoid_: Seed provider, mock provider

### Ingestion Concepts

**Source Topic**:
An external Kafka topic or future server-side source that provides messages to be mapped into a View Server Topic.
_Avoid_: View Server Topic

**Kafka Source Codec**:
A typed decoder contract for Kafka message keys and values before Mapping, such as protobuf, JSON, string, bytes, or a custom Effectful decoder. It is the source-format Seam; the View Server Topic schema remains the target truth.
_Avoid_: Topic schema, row schema, serializer

**Region**:
A named Kafka/source deployment location configured for ingestion.
_Avoid_: Location string, environment, cluster unless discussing infrastructure

**Mapping**:
The typed function that transforms a source message, key, region, schema, and metadata into a Topic Row for a View Server Topic.
_Avoid_: Serializer, mapper when it obscures the target Topic Row contract

**Publish**:
A server-side mutation that inserts or replaces a Topic Row in a View Server Topic.
_Avoid_: Browser write, send, emit

## Relationships

- A **View Server** owns one or more **View Server Topics**.
- A **View Server Topic** has exactly one configured **Row Key**.
- A **Topic Row** belongs to exactly one **View Server Topic**.
- A **Live Query** targets exactly one **View Server Topic**.
- A **Raw Query** returns selected Topic Row fields.
- A **Grouped Query** returns group fields plus aggregate aliases.
- A **Subscription** belongs to one **Live Query** and emits one **Snapshot** followed by zero or more **Deltas** and **Status Events**.
- A **Column Live View Engine** owns one **Columnar Topic Store** per **View Server Topic**.
- A **Columnar Topic Store** owns one **Topic Column Vector** per configured Topic Row field.
- A **Runtime Core** owns one **Column Live View Engine** instance and exposes both a **Runtime Client** and a **Live Client**.
- A **Raw Query Plan** is compiled once from a **Raw Query** before the **Columnar Topic Store** scans rows.
- A **Raw Predicate Plan** is part of a **Raw Query Plan** and lets storage narrow scans without replacing the correctness callback unless it is proven exact.
- A **Columnar Topic Store** may maintain **Raw Ordered Window Indexes** to accelerate bounded **Raw Query** windows.
- A **Grouped Query Plan** is compiled once from a **Grouped Query** before grouped full-scan or incremental execution.
- An **Active Query** may serve many equivalent **Subscriptions**.
- A **Live Client** can subscribe to **Live Queries** but cannot publish mutations.
- A **Runtime Client** can publish mutations but is not exposed to browsers by the Real View Server.
- A **Remote Browser Client** is a **Live Client** adapter for the **Wire Protocol**.
- A **Field Filter Codec** protects the **Wire Protocol** from unsafe or incorrectly typed filter values.
- A **Raw Query Codec** protects Raw Query wire payloads from unknown fields, unsafe filters, and invalid windows.
- A **Grouped Query Codec** protects Grouped Query wire payloads from invalid group fields, aggregate aliases, aggregate fields, grouped ordering, and invalid windows.
- An **Aggregate Row Codec** protects grouped aggregate row values from JSON precision loss over the **Wire Protocol**.
- A **Health Summary Codec** protects the compact health summary stream from impossible status combinations and unknown unhealthy topic names.
- A **Health Topic Codec** protects the per-topic health stream from missing, duplicate, unknown, or mismatched topic rows.
- A **Health Payload Codec** protects full runtime health payloads from missing or unknown configured topics.
- A **View Server Provider** supplies a **Live Client** to React hooks.
- A **View Server In-Memory Provider** supplies the same hook behavior through an **In-Memory View Server**.
- A **Real View Server** and **In-Memory View Server** differ only by transport and ingress **Adapters**, not by query, storage, health, or subscription logic.
- A **Source Topic** uses one **Kafka Source Codec** for its value and optionally one **Kafka Source Codec** for its key.
- A **Source Topic** is mapped into a **View Server Topic** through a **Mapping**.
- **Health Ledger** state feeds engine health, runtime health, transport health, and React health.

## Example Dialogue

> **Dev:** "Can the browser publish an **Order** row through the **Remote Browser Client**?"
>
> **Domain expert:** "No. The browser only uses the **Live Client** side: it starts a **Live Query** and receives a **Snapshot**, **Deltas**, and **Status Events**. Server-side ingestion uses a **Runtime Client** or runtime adapters to **Publish** rows."
>
> **Dev:** "For tests, should we mock the hook?"
>
> **Domain expert:** "No. Use the **View Server In-Memory Provider**. It gives the same hook behavior as the **View Server Provider**, backed by an **In-Memory View Server** and the real **Column Live View Engine**."

## Flagged Ambiguities

- "topic" can mean **Source Topic** or **View Server Topic**. Use the full term when ingestion is involved.
- "client" can mean **Live Client**, **Runtime Client**, or **Remote Browser Client**. Use the precise term because each has different mutation permissions.
- "provider" can mean **View Server Provider** or **View Server In-Memory Provider**. Use the precise term when ownership/cleanup matters.
- "protocol" means the **Wire Protocol** unless explicitly discussing an internal TypeScript interface.
- "subscription" is not a WebSocket connection; a single connection can carry multiple **Subscriptions**.
- "health" should specify **Health Ledger**, engine health, runtime health, transport health, or React health when the owner matters.
- "view" is overloaded in database/UI language; prefer **Live Query**, **Snapshot**, or **Grouped Query** depending on the intended concept.
