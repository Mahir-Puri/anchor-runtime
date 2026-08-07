# Architecture

## The problem

An agent run is a long chain of side effects driven by a model. It calls a tool,
gets a result, calls another, and somewhere in the middle the process dies:
deploy, OOM, spot reclaim, node drain. The usual answer is to retry the whole
run, which re-sends the email and re-issues the refund, or to give up on it,
which strands the customer. Neither is acceptable once real money is involved.

Anchor treats a run as a state machine whose transitions are written down before
they are attempted. Recovery is then a matter of reading what was written.

## Components

```
                    ┌──────────────────────────────┐
   client ─────────►│  API (FastAPI)               │
   POST /v1/runs    │  idempotent submit           │
   Idempotency-Key  │  cancel · read · /metrics    │
                    └───────────────┬──────────────┘
                                    │ INSERT run + queue row + RunStarted
                                    ▼
        ┌───────────────────────────────────────────────────────┐
        │  Postgres                                             │
        │                                                       │
        │   runs        one row per run, plus a usage cache      │
        │   events      append-only log, (run_id, seq) PK        │
        │   run_queue   claimable rows with a time-based lease   │
        │   side_effects  stand-in for a downstream system       │
        └───────────────────────────┬───────────────────────────┘
                                    │ FOR UPDATE SKIP LOCKED
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
            ┌───────────────┐               ┌───────────────┐
            │  Worker 1     │               │  Worker N     │
            │  claim        │               │  claim        │
            │  heartbeat    │      ...      │  heartbeat    │
            │  drain on TERM│               │               │
            └───────┬───────┘               └───────────────┘
                    │
                    ▼
            ┌───────────────────────────────────────────┐
            │  Engine (one attempt of one run)          │
            │   load events ─► rebuild step state       │
            │   run workflow ─► terminal status         │
            └───────────────────┬───────────────────────┘
                                ▼
            ┌───────────────────────────────────────────┐
            │  RunContext                               │
            │   ctx.step / ctx.call_tool / ctx.model     │
            │   ctx.gather                              │
            │                                           │
            │   per boundary: lease · cancel · budget    │
            └───────────────────┬───────────────────────┘
                                ▼
                    model provider   ·   registered tools
```

## The step lifecycle

Every durable operation follows the same five moves:

```
   step reached
        │
        ├── result already in the log?  ──► return it, execute nothing
        │
        ├── check lease · check cancel · check budget
        │
        ├── write StepStarted{token}      ◄── the crash window opens here
        │
        ├── execute (with in-attempt retries)
        │
        └── write StepCompleted{result}   ◄── the crash window closes here
```

A worker that dies between StepStarted and StepCompleted leaves a step whose
outcome nobody knows. That case is the whole design; see FAILURE_MATRIX.md.

## Why the log is the source of truth

Everything on the `runs` row could be rebuilt by reading the events for that run.
Status, usage, result: all caches. Keeping it that way means replay never has to
reconcile two stories about what happened, and it means the log can be read
directly when something goes wrong, which is what an operator actually wants at
3am.

Sequence numbers come from a single statement that bumps `runs.event_seq` and
inserts the event together, so they are gapless without a distributed lock. The
`(run_id, seq)` primary key then acts as a tripwire: if two workers ever believe
they own the same run, the second write fails loudly instead of quietly
interleaving.

## Why leases instead of visibility timeouts in code

A worker holds a run by writing `locked_until` into the future and refreshing it
every few seconds. There is no crash-recovery code path anywhere in the worker,
because recovery is the _absence_ of a heartbeat: the lease lapses, another
worker's `claim` finds the row eligible, and the engine replays from the log.
`kill -9` and a clean shutdown converge on the same outcome, which is why the
chaos script can prove something.

Recovery time is therefore bounded by lease length, not by anything clever.
Shorten the lease for faster failover and more heartbeat traffic.

## Determinism, and its one sharp edge

Step keys are positional: the Nth call to a step named `refund` is `refund#N`.
Authors never invent identifiers, which removes a whole class of copy-paste bugs,
but it means the workflow must reach the same step names in the same order on
every attempt.

Branching on step _results_ is fine, because results are replayed from the log.
Branching on anything the log does not capture (wall-clock time, `random`, a
live read outside a step) is not. Wrap it in a step and it becomes safe. This is
the same constraint Temporal and DBOS impose, for the same reason.

## Portability

The engine talks to a `Store` protocol that needs exactly five things:

1. an atomically-sequenced append-only log per run
2. a compare-and-set on run status
3. a queue with an atomic claim and a time-based lease
4. a unique-key insert, for submission idempotency
5. a full read of one run's log

Postgres provides all five directly. DynamoDB provides them with a conditional
write on a `(run_id, seq)` composite key plus a GSI for the queue. Cosmos DB
provides them with an ETag precondition on the run document. The surface is kept
this small deliberately: it is the difference between portable and portable in
principle.
