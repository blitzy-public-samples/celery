# Global Rate Limiter — Metrics & Health Reference

This document is the canonical telemetry reference for the **opt-in, Redis-backed global (cross-worker) rate limiter** added by this feature. The limiter is implemented in `celery/worker/global_ratelimit.py` (the `GlobalRateLimiter` manager + the `GlobalTokenBucket` wrapper) and is engaged through the single hook `bucket_for_task()` in `celery/worker/consumer/consumer.py` when `worker_enable_global_rate_limits` is `True`.

It exists to satisfy the Observability rule (AAP §0.8.2.2) and, for **each** signal, **clearly delineates REUSED telemetry (existing Celery infrastructure) from ADDED telemetry (new in this feature)**.

> **No HTTP metrics endpoint. No events-bus wiring.** Celery exposes **no** metrics HTTP endpoint, and this feature adds none (AAP §0.7.2, §0.8.3). All ADDED telemetry below is **in-module only**: plain in-process counters, a structured log line, and an optional health-check method. Operators wire these into their **own** pipeline (e.g. a statsd/Prometheus exporter that reads the counters, or a log-derived metric for the WARNING line). The companion dashboard, [`grafana-dashboard.json`](./grafana-dashboard.json), is a **template** whose PromQL expressions (`celery_global_rate_limit_*`) are illustrative, **operator-supplied** placeholders — not metrics emitted by Celery core.

---

## 1. Signal summary

| Signal | REUSED / ADDED | Type | Meaning | Dashboard panel |
|--------|----------------|------|---------|-----------------|
| `celery.utils.log.get_logger(__name__)` module logger | **REUSED** | Log stream (stdlib `logging` via Celery) | Celery's existing logger hierarchy (roots in the `celery` logger, wrapping kombu's logger). Carries all limiter log records with **structured, correlatable fields**: the **task name** and the **bucket key** `celery:globalratelimit:<task>:<rate>`. No new logging framework is introduced. | (log stream; feeds *Fail-open Warning Rate* when scraped as a log-derived metric) |
| `GlobalRateLimiter.allow_count` | **ADDED** | Counter (int, monotonic per process) | Incremented via `_incr_allow()` each time the atomic Redis Lua script grants a token (`allowed=1`). Aggregates per-task allow outcomes across the worker process. | *Total Allowed*; *Token Acquisition Rate (allow / deny / fallback)* |
| `GlobalRateLimiter.deny_count` | **ADDED** | Counter (int, monotonic per process) | Incremented via `_incr_deny()` each time the script refuses a token (`allowed=0`); the task is deferred through the existing per-worker retry loop (unchanged). | *Total Denied*; *Token Acquisition Rate (allow / deny / fallback)* |
| `GlobalRateLimiter.fallback_count` | **ADDED** | Counter (int, monotonic per process) | Incremented via `_incr_fallback()` on any Redis error (or while in fail-open mode), when the limiter delegates the decision to the local per-worker `TokenBucket`. This is the same event as the fail-open WARNING log line. | *Total Fallback (fail-open)*; *Token Acquisition Rate (allow / deny / fallback)*; *Fail-open Warning Rate* |
| Fail-open **WARNING** log line | **ADDED** (message) over **REUSED** logger | Log event (`WARNING`) | Emitted via the reused module logger when Redis is unavailable/errors: `Global rate limiter unavailable for task %s (key=%s): %r; falling back to local per-worker bucket.` Includes the **task name** and **bucket key**, and signals that the limiter fell back to the local per-worker `TokenBucket`. | *Fail-open Warning Rate* |
| `GlobalRateLimiter.ping()` | **ADDED** | Health check (`bool`) | Optional connectivity/readiness probe: returns `bool(self.client.ping())`; returns `False` on any exception or while in fail-open mode. Not auto-invoked anywhere in Celery core — for operator readiness checks only. | *Limiter Connectivity / Readiness*; *Limiter Readiness (ping)* |

---

## 2. REUSED telemetry (existing Celery infrastructure)

The limiter introduces **no new logging framework**. It obtains its logger exactly as other worker modules do:

```python
from celery.utils.log import get_logger
logger = get_logger(__name__)
```

`celery.utils.log.get_logger` wraps kombu's `get_logger`, so the limiter's logger is part of Celery's existing **`celery` logger hierarchy** and inherits the worker's configured handlers, formatters, and levels. Every limiter log record carries **structured, correlatable fields**:

- **task name** — the rate-limited task the decision applies to.
- **bucket key** — `celery:globalratelimit:<task>:<rate>`, where `<rate>` is the normalized tokens-per-second fill rate parsed from the task's `rate_limit` by Celery's existing `rate()` utility. The key embeds both task name and rate, so distinct tasks (and distinct rates) never share a bucket.

Because this is the standard Celery logging path, these records flow to whatever sinks the operator already configured (console, files, or a log shipper). No code change to Celery's logging is made.

## 3. ADDED telemetry (new in this feature, in-module only)

### 3.1 Allow / Deny / Fallback counters
The `GlobalRateLimiter` manager holds three plain integer counters, initialized to `0`, incremented through internal helpers:

| Counter | Helper | Incremented when |
|---------|--------|------------------|
| `allow_count` | `_incr_allow()` | Atomic Lua script returns `allowed=1` (token granted). |
| `deny_count` | `_incr_deny()` | Atomic Lua script returns `allowed=0` (token refused; task deferred via the existing retry loop). |
| `fallback_count` | `_incr_fallback()` | Any Redis error / fail-open mode → decision delegated to the local per-worker `TokenBucket`. |

Per-task `GlobalTokenBucket` instances call back into the manager, so these counters **aggregate per-task token-acquisition outcomes across the worker process**. They are in-process integers — operators export them via their own exporter (mapped, illustratively, to the `celery_global_rate_limit_allow_total` / `_deny_total` / `_fallback_total` placeholder series on the dashboard).

### 3.2 Fail-open WARNING log line
When Redis is unreachable or errors during a token check, the limiter is **fail-open**: it logs a WARNING and falls back to the local per-worker `TokenBucket` so task processing is **never blocked** (no task is dropped, rejected, or dead-lettered). The message is:

```
Global rate limiter unavailable for task %s (key=%s): %r; falling back to local per-worker bucket.
```

It is emitted on the REUSED module logger and includes the task name and bucket key. Each occurrence coincides with a `fallback_count` increment, so the *Fail-open Warning Rate* panel can be driven either from `fallback_count` or from a log-derived metric that counts this WARNING.

### 3.3 Connectivity / readiness ping
`GlobalRateLimiter.ping()` returns a `bool` reflecting limiter→Redis connectivity (`bool(self.client.ping())`), returning `False` on any exception or while in fail-open mode. It is provided for operator **readiness/health checks** and is **not** auto-invoked by Celery core. On the dashboard it maps (illustratively) to `celery_global_rate_limit_up` (1=UP / 0=DOWN).

---

## 4. Dashboard mapping

The signals above map to panels in [`grafana-dashboard.json`](./grafana-dashboard.json). The dashboard is a portable **template** (datasource template variable `DS_PROMETHEUS`); its PromQL expressions are operator-supplied placeholders.

| Dashboard panel | Visualizes | Placeholder expression(s) |
|-----------------|-----------|---------------------------|
| *Total Allowed* | `allow_count` (current total) | `sum(celery_global_rate_limit_allow_total)` |
| *Total Denied* | `deny_count` (current total) | `sum(celery_global_rate_limit_deny_total)` |
| *Total Fallback (fail-open)* | `fallback_count` (current total) | `sum(celery_global_rate_limit_fallback_total)` |
| *Token Acquisition Rate (allow / deny / fallback)* | per-second rate of all three counters | `rate(celery_global_rate_limit_allow_total[$__rate_interval])`, `…deny…`, `…fallback…` |
| *Fail-open Warning Rate* | rate of fail-open events / WARNING log | `rate(celery_global_rate_limit_fallback_total[$__rate_interval])` |
| *Limiter Connectivity / Readiness* | `ping()` readiness (stat, UP/DOWN) | `min(celery_global_rate_limit_up)` |
| *Limiter Readiness (ping)* | `ping()` readiness (gauge, UP/DOWN) | `min(celery_global_rate_limit_up)` |

---

## 5. Validation in local development

This telemetry is **exercised end-to-end in the local development environment** by the integration test `t/integration/test_global_rate_limit.py`, which runs against a live Redis using the project's existing integration fixtures (the `flaky` fixture — `pytest.mark.flaky(reruns=5, reruns_delay=1)` plus a timeout) and a **shared Redis counter** to tally executions. It verifies multi-worker aggregate throughput (`N >= 2`) stays `<= R` and single-worker parity (`N = 1`). The unit suite `t/unit/worker/test_global_ratelimit.py` asserts the counters increment on allow/deny and that the fail-open **WARNING** is logged (via `caplog`) with `fallback_count` incremented when Redis errors.

## 6. Scope note

All ADDED telemetry is **in-module only** — counters, a log line, and `ping()`. **No HTTP metrics endpoint and no signals/events-bus integration are added to Celery core** (AAP §0.7.2, §0.8.3). The limiter does not modify Celery's logging configuration; it only reuses it. Operators are responsible for wiring the in-module counters/logs into their own metrics pipeline; the dashboard template documents one illustrative Prometheus-based wiring.
