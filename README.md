# Anchor

**A durable execution runtime for LLM agents. Kill the worker mid-refund and the customer still gets refunded exactly once.**

[![ci](https://img.shields.io/badge/tests-69%20passing-brightgreen)](.github/workflows/ci.yml)
[![chaos](https://img.shields.io/badge/chaos-SIGKILL%20verified-blue)](scripts/chaos_kill.py)
[![python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml)
[![license](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

---

## The problem

An agent run is a long chain of side effects driven by a model. It looks up a
payment, issues a refund, sends an email. Somewhere in the middle the process
dies, because processes die: a deploy, an OOM, a spot reclaim, a node drain.
Now you have two bad options. Retry the whole run and you refund twice. Give up
and the customer never gets their money. Most agent frameworks hand you exactly
these two options and call it a day.

Anchor writes down every step before it attempts it, so recovery is a matter of
reading what was written instead of guessing what happened.
