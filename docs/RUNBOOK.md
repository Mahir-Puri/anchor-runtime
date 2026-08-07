# Runbook

For whoever is on call, including future me.

## Dashboards

`/metrics` exposes two gauges in Prometheus format:

- `anchor_runs_total{status=...}` — runs by status
- `anchor_queue_depth{state=claimable|leased|total}` — queue state

## Alerts worth having

| Alert               | Condition                                                     | Why it matters                                                      |
| ------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------- |
| Runs needing review | `anchor_runs_total{status="NEEDS_REVIEW"} > 0`                | A side effect with an unknown outcome is sitting there. Page.       |
| Queue backing up    | `anchor_queue_depth{state="claimable"}` rising for 5m         | Workers are down, wedged, or under-provisioned.                     |
| Nothing leased      | `anchor_queue_depth{state="leased"} == 0` while claimable > 0 | No worker is claiming. Deploy failure or a bad database credential. |
| Failure rate        | `rate(anchor_runs_total{status="FAILED"})` above baseline     | A downstream dependency is probably down.                           |
| Health failing      | `/healthz` non-200                                            | It touches the database, so this is a real dependency check.        |

## Five things that go wrong

### 1. A run is in NEEDS_REVIEW

**What happened.** A worker died inside an at-most-once step that has no
verifier. The runtime refused to guess whether the effect landed.

**What to do.**

```bash
anchor tail <run_id>          # find the RunNeedsReview event
```

The event payload carries `step_key` and `token`. Search the downstream system
for that token. It either landed or it did not; you now know which, and the
runtime never had to guess.

**How to stop it recurring.** Give the tool a `verify` function. That moves the
case from row 4 of the failure matrix to row 3 and it resolves itself next time.

### 2. Queue depth is climbing

Check workers are alive and claiming:

```bash
curl -s localhost:8000/healthz | jq .queue
docker compose logs --tail=50 worker
```

If `leased` is 0 while `claimable` is large, no worker is picking up work: check
`ANCHOR_DATABASE_URL` and that the workers have the workflow code deployed (a
worker missing the workflow fails runs fast rather than silently skipping them,
so you would also see FAILED runs with `WorkflowNotFound`).

If `leased` is high and stable but nothing completes, workers are stuck inside
long steps. Look at the newest `StepStarted` events with no matching
`StepCompleted`.

### 3. Recovery after a crash is taking too long

Recovery is bounded by `ANCHOR_LEASE_SECONDS`, because that is how long a dead
worker's lease takes to lapse. Measured on the chaos script: with a 5s lease,
mean recovery was 5.19s. Shorten the lease for faster failover at the cost of
more heartbeat traffic. Do not shorten it below the longest gap between step
boundaries or healthy workers will lose leases mid-step.

### 4. Runs are failing on budget

The error payload names the `dimension` (`tokens`, `tool_calls`, `model_calls`,
`wall_seconds`, `cost_usd`). If it is `wall_seconds` on a run that restarted
several times, that is working as intended: the clock runs from run creation so a
crash-loop cannot buy itself more time. Raise the ceiling per submission rather
than globally.

### 5. A poison run is crash-looping a worker

A run whose workflow reliably kills the process will be reclaimed forever.
`run_queue.attempts` counts deliveries; find repeat offenders:

```sql
SELECT run_id, attempts FROM run_queue WHERE attempts > 5 ORDER BY attempts DESC;
```

Cancel it (`POST /v1/runs/{id}/cancel`) and fix the workflow. A delivery ceiling
that moves such runs to a dead-letter status is the obvious next feature.

## Deploys

Workers drain on SIGTERM: they stop claiming and finish in-flight runs. Give
containers a termination grace period longer than your slowest step, otherwise
the orchestrator SIGKILLs mid-step and every interrupted run pays for a replay.
Recoverable, but avoidable.

`anchor migrate` is idempotent and safe to run on every deploy.

## Useful queries

```sql
-- runs stuck RUNNING with no lease (their worker vanished, awaiting reclaim)
SELECT r.run_id, r.updated_at, q.locked_until
  FROM runs r JOIN run_queue q USING (run_id)
 WHERE r.status = 'RUNNING' AND (q.locked_until IS NULL OR q.locked_until < now());

-- steps that opened a crash window and never closed it
SELECT e.run_id, e.step_key, e.created_at
  FROM events e
 WHERE e.type = 'StepStarted'
   AND NOT EXISTS (
       SELECT 1 FROM events c
        WHERE c.run_id = e.run_id AND c.step_key = e.step_key
          AND c.type IN ('StepCompleted', 'StepFailed'));
```
