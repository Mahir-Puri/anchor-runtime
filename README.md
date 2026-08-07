# Anchor

**A durable execution runtime for LLM agents. Kill the worker mid-refund and the customer still gets refunded exactly once.**

[![ci](https://img.shields.io/badge/tests-69%20passing-brightgreen)](.github/workflows/ci.yml)
[![chaos](https://img.shields.io/badge/chaos-SIGKILL%20verified-blue)](scripts/chaos_kill.py)
[![python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml)
[![license](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

---

## The problem, in one paragraph

An agent run is a long chain of side effects driven by a model. It looks up a
payment, issues a refund, sends an email. Somewhere in the middle the process
dies, because processes die: a deploy, an OOM, a spot reclaim, a node drain.
Now you have two bad options. Retry the whole run and you refund twice. Give up
and the customer never gets their money. Most agent frameworks hand you exactly
these two options and call it a day.

Anchor writes down every step before it attempts it, so recovery is a matter of
reading what was written instead of guessing what happened.

## The demo

This is the output of `python scripts/chaos_kill.py`, which submits a refund run,
waits until the refund has actually landed downstream, and then sends the worker
a `SIGKILL`. No shutdown hook, no final write, no cleanup. A fresh worker picks
up the pieces.

```
chaos: 3 trial(s), lease=5.0s, window=2.0s

  trial 1: status=COMPLETED  refunds_at_kill=1 refunds_final=1 attempts=2 recovery=5.23s
  trial 2: status=COMPLETED  refunds_at_kill=1 refunds_final=1 attempts=2 recovery=5.19s
  trial 3: status=COMPLETED  refunds_at_kill=1 refunds_final=1 attempts=2 recovery=5.16s

trials:              3
duplicate refunds:   0
unfinished runs:     0
steps recovered by verifier: 3
recovery seconds:    min=5.16 max=5.23 mean=5.19

RESULT: PASS
```

`refunds_at_kill=1` means the money had already moved when the process was
killed. `refunds_final=1` means it did not move again. The run finished anyway.

## Quick start

You need Docker and Python 3.11+.

```bash
git clone https://github.com/<you>/anchor-runtime && cd anchor-runtime
pip install -e ".[dev]"

make db        # start Postgres
make demo      # submit one refund run, work it, print the full event log
make chaos     # kill a worker mid-refund and check the invariant holds
make test      # 55 tests against a real Postgres
make test-dynamo  # 14 DynamoDB contract tests via moto (no credentials needed)
```

`make demo` prints the entire trajectory, which is the fastest way to understand
what the runtime is doing:

```
run 6a3c...  workflow=refund_agent  status=COMPLETED  attempt=1
------------------------------------------------------------------------
    1  RunStarted            -                        {"input": {"payment_id": "pay_001", ...
    2  StepStarted           model#1                  {"token": "d3cc6e84-3551-5c9b-..."}
    3  StepCompleted         model#1                  {"result": {"text": "calling lookup_pay...
    4  ModelCalled           model#1                  {"tool_calls": ["lookup_payment"], ...
    5  StepStarted           tool:lookup_payment#1    {"token": "bdf9cd6e-c588-54b1-..."}
    6  StepCompleted         tool:lookup_payment#1    {"result": {"captured_cents": 4999, ...
    7  ToolInvoked           tool:lookup_payment#1    {"idempotency": "at_least_once", ...
   ...
   11  StepStarted           tool:issue_refund#1      {"token": "1505a68c-01d8-56de-..."}
   12  StepCompleted         tool:issue_refund#1      {"result": {"created": true, ...
   ...
   23  RunCompleted          -                        {"usage": {"tool_calls": 3, ...

status=COMPLETED executed=7 replayed=0 refunds_recorded=1
```

Everything above runs with no API key. The default model provider is a
deterministic offline stub, which is what makes crash recovery reproducible.
Point it at a real model with `ANCHOR_MODEL_PROVIDER=anthropic`.

## Architecture

```
                    ┌──────────────────────────────┐
   client ─────────►│  API (FastAPI)               │
   POST /v1/runs    │  idempotent submit           │
   Idempotency-Key  │  cancel · read · /metrics    │
                    └───────────────┬──────────────┘
                                    │  INSERT run + queue row + RunStarted
                                    ▼
        ┌───────────────────────────────────────────────────────┐
        │  Postgres                                             │
        │                                                       │
        │   runs          one row per run (status, usage cache)  │
        │   events        append-only log, (run_id, seq) PK      │
        │   run_queue     claimable rows with a time-based lease │
        │   side_effects  stand-in for a downstream system       │
        └───────────────────────────┬───────────────────────────┘
                                    │  FOR UPDATE SKIP LOCKED
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
            ┌───────────────┐               ┌───────────────┐
            │  Worker 1     │      ...      │  Worker N     │
            │  claim        │               │  claim        │
            │  heartbeat    │               │  heartbeat    │
            │  drain on TERM│               │               │
            └───────┬───────┘               └───────────────┘
                    │
                    ▼
            ┌───────────────────────────────────────────┐
            │  Engine — one attempt of one run           │
            │    load events ──► rebuild step state      │
            │    run workflow ──► terminal status        │
            └───────────────────┬───────────────────────┘
                                ▼
            ┌───────────────────────────────────────────┐
            │  RunContext                                │
            │    ctx.step · ctx.call_tool · ctx.model     │
            │    ctx.gather                               │
            │                                             │
            │    every boundary: lease · cancel · budget   │
            └───────────────────┬───────────────────────┘
                                ▼
                    model provider   ·   registered tools
```

### The step lifecycle

Every durable operation follows the same five moves:

```
   step reached
        │
        ├── result already in the log?  ──► return it, execute nothing
        │
        ├── check lease · check cancel · check budget
        │
        ├── write StepStarted{token}      ◄── crash window opens
        │
        ├── execute (with in-attempt retries)
        │
        └── write StepCompleted{result}   ◄── crash window closes
```

A worker that dies between those last two lines leaves a step whose outcome
nobody knows. That case is the entire design, and it is handled below.

## Writing a workflow

```python
from anchor import workflow, tool, StepIdempotency

@tool("issue_refund",
      description="Refund a captured payment. Moves money.",
      idempotency=StepIdempotency.AT_MOST_ONCE,
      verify=refund_landed)                 # asks downstream: did my token land?
async def issue_refund(ctx, token, payment_id, amount_cents):
    return await payments.refund(payment_id, amount_cents, idempotency_key=token)

@workflow("refund_agent")
async def refund_agent(ctx, payload):
    messages = [{"role": "user", "content": json.dumps(payload)}]
    for _ in range(MAX_TURNS):
        response = await ctx.model(messages, tools=schemas)   # durable
        if not response.wants_tools:
            return {"summary": response.text}
        for call in response.tool_calls:
            await ctx.call_tool(call.name, call.arguments)    # durable
        ...
```

The agent loop is ordinary. That is the argument: durability lives in the
runtime, so the interesting code stays short. The one rule is that anything
reaching outside the workflow goes through `ctx`.

## Design decisions

### 1. The event log is the only truth

Status, usage, and results on the `runs` row are caches. Every one of them could
be rebuilt by reading that run's events. Replay never has to reconcile two
stories about what happened, and when something goes wrong you can read the log
directly, which is what you actually want at 3am.

Sequence numbers come from one statement that bumps a counter and inserts the
event together, so they are gapless without a distributed lock. The
`(run_id, seq)` primary key is a tripwire: if two workers ever think they own the
same run, the second write fails loudly instead of quietly interleaving.

### 2. Idempotency tokens are derived, not random

A step's token is `uuid5(namespace, run_id + step_key)`. Same run, same step,
same token, forever. The token a payment processor sees on a replay is
byte-identical to the one it saw the first time, so it can deduplicate for us.
This is the difference between hoping a retry is safe and knowing it is.

### 3. Tools declare whether they can be repeated, and it is mandatory

```python
AT_LEAST_ONCE   # safe to run again: reads, or writes keyed by our token
AT_MOST_ONCE    # not safe: moving money, sending mail, a partner API with no dedupe
```

There is no default. If nobody thought about it, the code does not compile into
something that silently does the dangerous thing.

### 4. When the outcome is unknown, the runtime refuses to guess

This is the decision I would most want to be asked about. A worker dies inside
an at-most-once step. Three things can happen:

| Situation                     | What Anchor does                                                                                                      |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| The tool has a **verifier**   | Ask the downstream system whether the token landed. Record the real outcome and carry on. No duplicate, no lost work. |
| The step is **at-least-once** | Run it again. The token has not changed, so downstream deduplicates.                                                  |
| **Neither**                   | Park the run in `NEEDS_REVIEW` with the step key and token written to the record.                                     |

The third row is the honest one. The only two guesses available are "charge the
customer twice" and "silently drop their refund", and picking one on a
customer's behalf is not the runtime's call to make. A human gets the exact token
to look up. Adding a verifier to that tool later moves the case from row three to
row one without touching the workflow.

### 5. There is no crash-recovery code path

Workers hold runs with a lease they refresh every few seconds. A dead worker
stops refreshing, the lease lapses, and the next worker to poll claims the run
and replays it from the log. Recovery is the _absence_ of a heartbeat, not a
special branch. `kill -9` and a clean shutdown converge on the same outcome,
which is exactly why the chaos script proves something.

Recovery time is bounded by lease length. Nothing clever, and easy to tune.

### 6. Budgets live in the runtime, not the agent

Token, cost, tool-call, model-call and wall-clock ceilings are checked at step
boundaries and persisted as they accrue, because the agent code is the thing that
just crashed. Wall clock is measured from run creation, so a run that restarts
five times gets one budget between all five attempts rather than five budgets.
Replayed steps are not re-billed.

### 7. Submission is idempotent before the engine ever sees it

`POST /v1/runs` takes an `Idempotency-Key`. A client that times out and retries
gets the original run back with a `200` instead of a second run with a `201`.
This is the first place exactly-once can be lost, and it happens before any of
the clever machinery gets a chance to help.

### 8. Step keys are positional, and that has a cost

The Nth call to a step named `refund` is `refund#N`. Authors never invent
identifiers, which removes a class of copy-paste bugs. The cost is that a
workflow must reach the same step names in the same order every attempt.
Branching on step _results_ is fine, since results are replayed. Branching on
wall-clock time or `random()` outside a step is not. Wrap it in a step and it is
safe again. Temporal and DBOS impose the same constraint for the same reason, and
it is documented rather than hidden.

### 9. Tests run against a real Postgres

The behaviour under test is `FOR UPDATE SKIP LOCKED`, lease expiry, and
unique-constraint collisions. A mock of those is a mock of the exact thing that
would break in production. CI spins up Postgres 16 as a service container and
runs the chaos script on every commit.

## What is verified

69 tests (55 Postgres + 14 DynamoDB via moto), plus a chaos script that kills real processes. The full table lives in
[docs/FAILURE_MATRIX.md](docs/FAILURE_MATRIX.md); the ones worth knowing about:

| Failure                                    | Outcome                                             | Test                                                           |
| ------------------------------------------ | --------------------------------------------------- | -------------------------------------------------------------- |
| Worker killed between steps                | Completed steps replay, run finishes                | `test_completed_steps_are_not_re_executed`                     |
| Worker killed mid-refund, verifier present | Verifier recovers the real outcome, no duplicate    | `test_at_most_once_with_verifier_recovers_without_duplicating` |
| Worker killed mid-send, no verifier        | `NEEDS_REVIEW`, body never re-runs                  | `test_at_most_once_without_verifier_parks_for_review`          |
| Ten workers race for one run               | Exactly one claim wins                              | `test_concurrent_claims_never_overlap`                         |
| Slow worker's lease stolen                 | Stale worker stands down at its next boundary       | `test_stale_worker_stands_down_and_the_new_owner_finishes`     |
| Worker killed mid-fan-out                  | Finished branches replay, unfinished ones re-run    | `test_only_unfinished_branches_re_execute_after_a_crash`       |
| Run crash-loops                            | One budget across all attempts, not one per attempt | `test_budget_survives_a_restart`                               |
| Rolling deploy (SIGTERM)                   | Drains in-flight runs before exiting                | `test_worker_drains_in_flight_runs_on_shutdown`                |

## Measured

```
python scripts/loadtest.py --runs 200 --workers 4 --concurrency 8

worked 200/200 runs in 9.37s

throughput:          21.4 runs/s
event writes:        4600 (491/s)
submit  p50 / p99:   2.1 / 3.4 ms
e2e     p50 / p99:   7244 / 9185 ms
```

**Methodology and caveats, because a number without them is decoration.**
Measured on a single Linux container with Postgres 16 on the same host, Python
3.12, four worker processes at concurrency 8, using the deterministic offline
model provider. Each run performs 4 model calls and 3 tool calls and writes 23
events, so 200 runs is 4,600 event-log writes.

The offline provider returns in microseconds where a real model takes seconds.
That is deliberate: it isolates the cost of durability from the cost of
inference, which would otherwise drown the signal entirely. Read the throughput
number as "the runtime is not the bottleneck", not as a capacity plan.

End-to-end p50 is high because all 200 runs are submitted before any worker
starts, so it is mostly queue wait. Recovery time, measured separately by the
chaos script, averaged 5.19s against a 5s lease, which is the expected result:
failover is dominated by lease expiry.

## Layout

```
src/anchor/
  models.py      value types: Budget, Usage, RunStatus, StepIdempotency
  events.py      event vocabulary and token derivation
  budget.py      ceilings, checked at step boundaries
  registry.py    @workflow and @tool decorators
  context.py     the heart: durable steps, replay, the crash-window decision
  engine.py      one attempt of one run, terminal-state decisions
  worker.py      claim loop, lease heartbeat, graceful drain
  api.py         FastAPI control plane, /healthz, /metrics
  cli.py         migrate · worker · api · submit · tail · demo
  store/         Store protocol + PostgresStore + DynamoDBStore (SQS lease)
  providers/     base · echo (deterministic) · anthropic
  examples/      refund agent: read-only, money-moving, and unverifiable tools

scripts/
  chaos_kill.py  SIGKILLs workers mid-refund, asserts exactly-once
  loadtest.py    throughput and percentiles

docs/
  ARCHITECTURE.md    components, step lifecycle, portability surface
  FAILURE_MATRIX.md  16 failure modes and the test for each
  RUNBOOK.md         alerts, five things that go wrong, useful SQL
```

## Configuration

Every value has a working default. See [.env.example](.env.example).

| Variable                    | Default                                              | Notes                                           |
| --------------------------- | ---------------------------------------------------- | ----------------------------------------------- |
| `ANCHOR_DATABASE_URL`       | `postgresql://postgres:anchor@localhost:5432/anchor` |                                                 |
| `ANCHOR_LEASE_SECONDS`      | `30`                                                 | Bounds recovery time after a hard kill          |
| `ANCHOR_HEARTBEAT_SECONDS`  | `5`                                                  | Must be comfortably below the lease             |
| `ANCHOR_WORKER_CONCURRENCY` | `4`                                                  | Runs in flight per worker process               |
| `ANCHOR_STEP_MAX_ATTEMPTS`  | `3`                                                  | In-attempt retries for transient errors         |
| `ANCHOR_MODEL_PROVIDER`     | `echo`                                               | `echo` or `anthropic`                           |
| `ANCHOR_ECHO_PLAN`          | _(empty)_                                            | Scripted tool sequence for the offline provider |
| `ANTHROPIC_API_KEY`         | —                                                    | Only needed for `anthropic`                     |

## Portability

The engine talks to a `Store` protocol that needs five things: an atomically
sequenced append-only log, a compare-and-set on status, a queue with an atomic
claim and a lease, a unique-key insert, and a full read of one run's log.

Both backends are implemented and verified by the same 12-assertion contract
test suite.

| Capability                         | Postgres                                         | DynamoDB + SQS                                                      |
| ---------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------- |
| Append-only event log, gapless seq | CTE bumps `event_seq` + inserts in one statement | Conditional update on `COUNTER` item, `(run_id, seq)` composite key |
| Compare-and-set on status          | `UPDATE ... WHERE status = ?`                    | `update_item` with `ConditionExpression` on current status          |
| Leased queue                       | `FOR UPDATE SKIP LOCKED`, `locked_until` column  | SQS visibility timeout; heartbeat via `change_message_visibility`   |
| Submission idempotency             | `UNIQUE(idempotency_key)` index                  | `attribute_not_exists(pk)` conditional put on an `IDEM#` item       |
| Exactly-once side effects          | `UNIQUE(token)` index                            | `attribute_not_exists(pk)` conditional put on an `EFFECT#` item     |

Cosmos DB would map to the same five capabilities with ETag preconditions.

## Roadmap

- [x] `DynamoDBStore` (AWS: DynamoDB + SQS) — same 14-test contract suite as Postgres
- [ ] `CosmosStore` (Azure: Cosmos DB, Service Bus, Container Apps)
- [ ] Same test suite green against all three backends
- [ ] Expose tools over MCP instead of a bespoke registry
- [ ] Delivery ceiling with a dead-letter status for poison runs
- [ ] Durable `ctx.sleep` for workflows that wait on humans

## Prior art

[Temporal](https://temporal.io), [Restate](https://restate.dev) and
[DBOS](https://dbos.dev) solve durable execution generally, and this borrows
their core idea. What Anchor adds is the part specific to agents: the crash
window around a model-chosen tool call, idempotency declared per tool, budgets
that survive restarts, and an explicit refusal to guess when the outcome of a
side effect is unknowable.

## License

MIT.
