# Commit-service simulator benchmark

Run the GatedCounter workload with the synchronous policy provider:

```bash
buck2 run fbcode//msl_infra/experimental/agentic/logact/commit_service/tests:sim_benchmark -- 1 --trace /tmp/commit-service-trace.json
```

Arguments are `[seed] [--workload KIND] [--rows PATH] [--trace PATH] [--sticky] [--no-cache] [--buffered]`. The default seed is `1`.
Omit `--rows PATH` and `--trace PATH` to print only the latency and
observable-metrics reports.

Select the workload with `--workload shared-bus|independent-buses`:

- `shared-bus` (high contention; default): three counter clients each perform 40
  operations on the same bus. Every commit chooses uniformly among three
  persistent service instances using the simulator RNG. The gate-writes run
  adds a concurrent client performing four gate writes.
- `independent-buses` (low contention): each counter client has its own bus,
  gated counter, and policy register. It makes one call at a time. Every commit chooses uniformly
  among three persistent service instances using the simulator RNG. All three
  services handle all buses. With `--sticky`, client `i` uses service
  `(i % 3) + 1` for every commit on its bus, including commits after gate writes.
  Each of the three buses therefore uses a different persistent service.
  `--sticky` does not change `shared-bus` routing. The gate-writes run inserts four alternating gate
  writes into each client's counter sequence, one after every ten counter calls,
  so gate writes also obey the one-outstanding-call constraint.

For example:

```bash
buck2 run fbcode//msl_infra/experimental/agentic/logact/commit_service/tests:sim_benchmark -- 1 --workload independent-buses --rows /tmp/independent-buses.json
```

To pin each independent bus to one service:

```bash
buck2 run fbcode//msl_infra/experimental/agentic/logact/commit_service/tests:sim_benchmark -- 1 --workload independent-buses --sticky --rows /tmp/independent-buses-sticky.json
```

Routing is reported in console output and row-file metadata. Omitting `--sticky`
retains random routing in `independent-buses`.

Each service has its own AgentBus client and append cursor, engine runtime,
playback object, voter factory, decider factory, and policy providers. Durable
backing storage is shared; engine state and policy register keys are distinct
per bus in `independent-buses`.
Router copies refer to the same persistent service instances. The simulator,
observability sinks, and counter-outcome bookkeeping belong to the test harness.
Each bus is checked using the existing GatedCounter adapter and linearizability
model. Counter replay uses logged decisions for calls whose client responses
are still pending. Final verification uses a separate AgentBus client over the same durable
space. Initialization and validation do not contribute to the latency report.

Each service instance's AgentBus client reads its write-once space through a
private `CachingWriteOnceSpace` (LRU, 1,024 entries and 1 MiB of values, 64 KiB
per entry), shared only by that client's clones. Cache hits skip the backing
call and its delay; writes, misses, and tail discovery reach the backing space.
Each service also has a private `CachingStorage` for engine checkpoints over
shared backing storage, using production's defaults: a 60 s TTL, 1,024 entries,
and 1 MiB of values (64 KiB per entry). Successful checkpoint writes populate
the cache; a cache hit consumes that cached value and skips the backing `get`
and its delay. Reads do not populate the cache. Voter, decider, and policy-register
storage bypass the engine cache.

`--no-cache` sets both caches' entry limits to zero, keeping the same client
topology and workload. Console output and the `engine_storage_cache` metadata
in both JSON exports report engine caching and its limits.

## Buffered commits

`--buffered` wraps each persistent service instance in `BufferingCommitService`.
Commit calls to the same instance and bus share a FIFO queue across all router
handles. Different buses have separate queues; calls routed to different service
instances can still overlap on the same bus. The wrapper caches up to 1,024 bus
queues, which covers all buses in these workloads. Policy reads remain synchronous.

Buffering is disabled by default and works with every workload, `--sticky`, and
`--no-cache`. To queue a concurrent burst on each bus's fixed service:

```bash
buck2 run fbcode//msl_infra/experimental/agentic/logact/commit_service/tests:sim_benchmark -- 1 --workload independent-buses-concurrent --sticky --buffered --rows /tmp/buffered.json --trace /tmp/buffered-trace.json
```

The `service_1`..`service_3` observable decorators wrap the buffering layer, so
their commit latencies and trace spans include queue waits and service processing.
Counter latencies also include the adapter replay.
`create_bus` and AgentBus calls bypass the commit queues. Console output and the
`buffered` metadata in both JSON exports record whether buffering is enabled.

Backing policy-register and engine-storage `get`/`put` calls each have a 50 ms delay.
Engine checkpoint cache hits skip the backing `get` delay; writes always reach
backing storage.
AgentBus uses `WriteOnceAgentBus` over an in-memory `WriteOnceSpace`. Each
underlying space `read`, `write`, and `tail` call has a 50 ms delay. AgentBus calls
have no additional fixed delay: their latency follows from the backing calls,
including reads per log entry and retries for occupied slots.
The simulator adds scheduler jitter, so observed waits can exceed these delays.

## Observability

Instrumentation uses only the existing observable decorators, each sharing one
in-memory metrics store and row logger per run:

| Scope | Decorator | Wraps |
|---|---|---|
| `service_1`..`service_3` | `ObservableCommitSvc` | each persistent `CommitServiceV1` |
| `service_bus` | `ObservableAgentBus` | each service's AgentBus frontend |
| `client_bus` | `ObservableAgentBus` | the final-verification AgentBus frontend |
| `write_once` | `ObservableWriteOnceSpace` | every backing write-once space handle, beneath the cache |
| `engine_storage` | `ObservableStorage` | shared engine/voter/decider backing storage, beneath the engine checkpoint cache |
| `policy_register` | `ObservableStorage` | policy-register storage |

Because `write_once` and `engine_storage` sit beneath their caches, their counts
are backing I/O only; cache hits are not recorded.

Each scope prefixes its metric keys through `PrefixedMetrics` (for example
`service_1.commit_intention.commit_service.latency_us`) and adds a `scope`
field to its rows. After the latency report, the benchmark prints call counts,
error counts, and latency percentiles for every observed operation. Latencies
come from the decorators' `latency_us` histograms, in simulator time.

`service_bus` includes the GatedCounter adapter's replay, which reads through
the routed service's AgentBus frontend. `client_bus` is used only by the final
linearizability snapshot. Policy initialization bypasses the observable
storage, so `policy_register` counts only workload reads and gate writes.
Synchronous policy reads appear as `policy_register` `get` calls; there is no
separate policy-provider decorator.

Every decorator records its `latency_us` histogram and then logs its row with
no `await` in between, and the simulator clock only advances across awaits. The
benchmark's logger therefore reads the call's end time from the simulator clock
and takes its duration from the latency sample just recorded, giving exact
`start_us` and `end_us` in simulator monotonic microseconds.

`--rows PATH` writes every structured row as JSON, in call-completion order,
with the run name, scope, `api_call_name`, `start_us`, `end_us`, `latency_ms`,
error fields, and the decorator's additional fields.

`--trace PATH` writes the same rows as Chrome trace JSON, which loads in
Perfetto. Each run is a process and each scope is a track; begin-event
arguments hold the full row. Rows carry no parent links and the simulator does
not expose task identity, so nesting is not recorded: overlapping spans on a
track can belong to different clients, and a span's children must be inferred
from interval containment.

A synchronous policy lookup includes a policy-register `get`. A gate write
includes one policy-register `get` and one CAS `put`, so it takes at least
`2 × 50 ms = 100 ms`.
