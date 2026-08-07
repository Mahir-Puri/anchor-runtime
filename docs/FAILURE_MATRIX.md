# Failure matrix

What breaks, what the runtime does, and the test that pins it down.

| #   | Failure                                                   | Runtime response                                                                                               | Terminal state                      | Test                                                           |
| --- | --------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | ----------------------------------- | -------------------------------------------------------------- |
| 1   | Worker killed between steps                               | Lease expires, another worker claims, completed steps replay from the log                                      | COMPLETED                           | `test_completed_steps_are_not_re_executed`                     |
| 2   | Worker killed mid-step, step is at-least-once             | Step body runs again with the same token, so the downstream system dedupes                                     | COMPLETED                           | `test_at_least_once_step_is_simply_repeated`                   |
| 3   | Worker killed mid-step, at-most-once **with** verifier    | Verifier asks downstream whether the token landed; if yes, the real outcome is recorded and the run carries on | COMPLETED                           | `test_at_most_once_with_verifier_recovers_without_duplicating` |
| 4   | Worker killed mid-step, at-most-once **without** verifier | Outcome unknowable. Run parks with the token on the record for a human                                         | NEEDS_REVIEW                        | `test_at_most_once_without_verifier_parks_for_review`          |
| 5   | Client retries POST /v1/runs after a timeout              | Unique index on the idempotency key returns the original run, 200 instead of 201                               | unchanged                           | `test_submit_returns_201_then_200_for_a_retry`                 |
| 6   | Two workers race for the same queued run                  | `FOR UPDATE SKIP LOCKED` hands the row to exactly one                                                          | n/a                                 | `test_concurrent_claims_never_overlap`                         |
| 7   | Slow worker's lease is stolen                             | Stale worker fails its heartbeat at the next boundary and stops touching state                                 | run stays RUNNING for the new owner | `test_stale_worker_stands_down_and_the_new_owner_finishes`     |
| 8   | Transient tool error                                      | In-attempt retry with exponential backoff, recorded as StepRetried                                             | COMPLETED                           | `test_step_retries_transient_failures_then_succeeds`           |
| 9   | Persistent tool error                                     | Retries exhausted, failure recorded against the step key                                                       | FAILED                              | `test_step_exhausting_retries_fails_the_run`                   |
| 10  | Agent loops on tool calls                                 | Budget ceiling fires at a step boundary before the call is made                                                | FAILED (`dimension` in error)       | `test_tool_call_ceiling_fails_the_run`                         |
| 11  | Run crash-loops, eating its budget                        | Wall clock runs from run creation, and replayed steps are not re-billed                                        | FAILED                              | `test_budget_survives_a_restart`                               |
| 12  | Cancel arrives mid-run                                    | Observed at the next boundary; completed work is preserved, nothing new starts                                 | CANCELLED                           | `test_cancel_stops_the_run_at_the_next_boundary`               |
| 13  | Duplicate delivery of a finished run                      | No-op, workflow body never entered                                                                             | unchanged                           | `test_terminal_runs_are_not_re_executed`                       |
| 14  | Worker deployed without the workflow code                 | Fails fast and clears the queue row instead of spinning                                                        | FAILED                              | `test_worker_ignores_workflows_it_does_not_know`               |
| 15  | Worker killed mid-fan-out                                 | Finished branches replay, unfinished ones re-execute                                                           | COMPLETED                           | `test_only_unfinished_branches_re_execute_after_a_crash`       |
| 16  | Rolling deploy (SIGTERM)                                  | Stops claiming, drains in-flight runs, then exits                                                              | COMPLETED                           | `test_worker_drains_in_flight_runs_on_shutdown`                |

## Row 4 is the interesting one

Every durable execution system has a window between "the side effect happened"
and "we recorded that it happened". If the process dies inside it and the
downstream system cannot be asked what it saw, there are exactly two guesses
available: retry, and risk charging the customer twice; or skip, and risk
silently dropping their refund.

Anchor does not guess. The run parks in NEEDS_REVIEW with the step key and the
idempotency token written to the record, so a human can look up that exact token
downstream and resolve it. Adding a verifier to the tool later moves the same
case from row 4 to row 3 without touching the workflow.

This is a product decision as much as a technical one, and it is the honest one:
a runtime that silently picks a side is a runtime that will eventually pick wrong
on someone's money.
