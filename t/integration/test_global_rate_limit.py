"""Integration tests for the opt-in Redis-backed global (cross-worker) rate limiter.

These tests exercise the feature **end-to-end and behaviorally**: they enable the
master switch (``worker_enable_global_rate_limits``), run one or more *real*
workers, dispatch a rate-limited task, and count executions through a shared Redis
counter. They never import or assert on limiter internals — the dependency on
``celery/worker/global_ratelimit.py`` is purely a runtime one, reached through the
worker consumer's bucket construction.

Headline success criterion (AAP 0.8.1): a task configured at rate ``R`` executes at
an aggregate throughput across all workers of ``<= R``, for any worker count
``N >= 1``, when global rate limiting is enabled. With ``N`` independent per-worker
buckets the aggregate would instead scale toward ``N * R``; the global limiter
collapses that to a single shared budget of ``~R``.

Worker model — one OS process per worker
----------------------------------------
"Global *across workers*" means coordination across *separate worker processes*
(each with its own consumer). We therefore start every worker in its own OS
process via :func:`multiprocessing` using the ``spawn`` start method. This is both
the faithful model (genuinely separate consumers competing for one shared Redis
bucket) and the robust one: nesting multiple embedded in-process workers is
unreliable — the prefork pool forks from a multi-threaded process (deadlock-prone)
and multiple in-process consumers contend on a shared event loop and fail to shut
down cleanly. Each child process owns its full worker lifecycle and is terminated
independently, so there is no cross-worker fork/event-loop/shutdown contention.

Rate selection — the limit must *bind*
--------------------------------------
``R`` is deliberately small. A worker can execute this trivial task at hundreds per
second, but Celery's rate-limit *deferral* machinery caps saturated throughput well
below high configured rates. Choosing a small ``R`` (here ``2/s``) keeps the
configured rate the binding constraint, so the per-worker case can scale toward
``N * R`` while the global case stays at ``~R`` — giving the assertion real
discriminating power. The window is short-but-generous to amortize worker start-up
and stay non-flaky.

Skip behavior
-------------
The tests require a live Redis. The skip decision is made **inside a fixture at run
time** — never at module import — so ``pytest --collect-only`` still lists the tests
with return code 0 when redis-py (or a live Redis) is unavailable. Consequently
every module-level import below must remain free of any eager dependency on
redis-py.
"""

import contextlib
import multiprocessing
import os
import time

import pytest

from celery import shared_task
from t.integration.conftest import flaky
from t.integration.tasks import get_redis_connection

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------
COUNTER_KEY = 'global-rate-limit-count'            # shared cross-worker counter key
READY_KEY = 'global-rate-limit-ready'              # workers signal readiness here
RATE_PER_SECOND = 2                                # R (tokens/second) — kept low so the limit binds
RATE = f'{RATE_PER_SECOND}/s'                      # task ``rate_limit`` string
WINDOW = 10                                        # measurement window T (seconds)
DISPATCH_COUNT = 200                               # backlog >> N*R*WINDOW so workers never starve
WORKER_READY_TIMEOUT = 30                          # seconds to wait for workers to start consuming
BUCKET_KEY_PATTERN = 'celery:globalratelimit:*'    # limiter key namespace (cleanup only)


# ---------------------------------------------------------------------------
# Local rate-limited task
# ---------------------------------------------------------------------------
# Defined locally (NOT in t/integration/tasks.py) so this module is strictly
# additive. A ``@shared_task`` registers on every app — including the apps the
# spawned workers build — so the workers can execute it by name. The explicit,
# namespaced ``name`` makes the limiter's per-task bucket key deterministic and
# collision-free, and ``ignore_result`` avoids cluttering the result backend.
@shared_task(
    name='t.integration.test_global_rate_limit.global_ratelimited_count',
    rate_limit=RATE,
    ignore_result=True,
)
def global_ratelimited_count(redis_key=COUNTER_KEY):
    """Increment a shared Redis counter; rate-limited at ``RATE``."""
    get_redis_connection().incr(redis_key)


def num_workers_aggregate(rate_per_second, elapsed, num_workers):
    """Per-worker (non-global) upper bound on executions over ``elapsed`` seconds.

    With ``num_workers`` *independent* token buckets, each refilling at
    ``rate_per_second``, the aggregate that could execute scales toward
    ``num_workers * rate_per_second * elapsed``. The global limiter must keep the
    observed count well below this value.
    """
    return num_workers * rate_per_second * elapsed


def _max_allowed(elapsed):
    """Upper bound on *global* executions over ``elapsed`` seconds.

    The shared bucket grants at most ``capacity`` (here 1, the burst) plus
    ``R * elapsed`` refilled tokens. We allow ``2 * R`` extra (a generous burst +
    one window-second of slop) plus a small constant for timing jitter, ramp-up and
    the readiness settle. This stays comfortably below the per-worker aggregate
    (``~N * R * T`` for ``N >= 2``), so the bound still separates global from
    per-worker behavior.
    """
    return RATE_PER_SECOND * elapsed + 2 * RATE_PER_SECOND + 2


# Stabilization slop (seconds) subtracted from the measured window before the
# single-worker parity lower bound. The dispatch loop, the worker poll cadence and
# a possibly-empty initial bucket mean the first second or two under-counts; a
# generous slop keeps the floor robust on slow CI while still proving the limiter
# sustains ~R.
_PARITY_STABILIZATION_SLOP = 4.0


def _min_expected(elapsed):
    """Lower bound on single-worker executions over ``elapsed`` seconds.

    The shared bucket refills at ``R`` tokens/second, so a healthy single worker
    draining a saturated backlog executes ~``R * elapsed`` tasks. We subtract a
    fixed stabilization slop and floor at zero. This is the *parity* assertion: it
    proves enabling the global limiter preserves throughput near ``R`` (the same
    ~``R`` the local per-worker ``TokenBucket`` yields), not merely that the count
    stays under the upper bound. A limiter that severely under-throttles or barely
    executes anything falls below this floor and fails the test.
    """
    return RATE_PER_SECOND * max(0.0, elapsed - _PARITY_STABILIZATION_SLOP)


# ---------------------------------------------------------------------------
# Worker process entry point (must be top-level so it is picklable for ``spawn``)
# ---------------------------------------------------------------------------
def _worker_process(broker_url, backend_url, limiter_url, ready_key, run_seconds):
    """Run ONE embedded worker with global rate limiting enabled, in its own process.

    Signals readiness through Redis once the worker is consuming, then idles for
    ``run_seconds`` before the process exits. Living in a dedicated OS process makes
    this a genuinely separate consumer competing for the single shared Redis bucket,
    with no in-process fork/event-loop/shutdown contention with sibling workers.
    """
    import time as _time

    # Allow the embedded worker to run as root inside CI/containers.
    os.environ.setdefault('C_FORCE_ROOT', '1')

    # Importing this module registers ``global_ratelimited_count`` by name (via
    # shared_task), so the spawned worker can execute the dispatched messages.
    import t.integration.test_global_rate_limit  # noqa: F401
    from celery import Celery
    from celery.contrib.testing.worker import start_worker

    app = Celery('global_ratelimit_test_worker')
    app.conf.broker_url = broker_url
    app.conf.result_backend = backend_url
    # The consumer reads this flag while building ``task_buckets`` at start-up, so it
    # must be enabled here — in the worker — before the worker starts.
    app.conf.worker_enable_global_rate_limits = True
    app.conf.global_rate_limit_url = limiter_url
    app.conf.worker_hijack_root_logger = False
    app.finalize()

    # ``solo`` pool: no fork, minimal footprint. The limiter gates at the consumer
    # (this process) before pool execution, so the pool type does not affect global
    # enforcement semantics.
    with start_worker(app, pool='solo', concurrency=1, perform_ping_check=False,
                      loglevel=0, shutdown_timeout=15):
        try:
            get_redis_connection().incr(ready_key)
        except Exception:
            # Readiness signalling is best-effort; the parent also has a timeout.
            pass
        _time.sleep(run_seconds)


# ---------------------------------------------------------------------------
# Skip + enable fixture (run-time skip; save/restore config)
# ---------------------------------------------------------------------------
@pytest.fixture
def global_rate_limit_app(app):
    """Enable the global rate limiter against a live Redis for one test.

    The skip is decided here (run time), never at module import, so that
    ``--collect-only`` still lists the tests with RC=0 when redis-py or a live Redis
    is absent. The opt-in flag and limiter URL are saved and restored so the
    (default-off) feature never leaks into other session-scoped tests.
    """
    # Run-time skip if redis-py is not installed (keeps collection RC=0).
    pytest.importorskip('redis')

    conn = get_redis_connection()
    try:
        conn.ping()
    except Exception:
        pytest.skip('Live Redis required for the global rate limit integration test')

    # Isolation: reset the shared counter, readiness key, and any stale limiter
    # buckets so a previous run can never inflate this run's count.
    conn.delete(COUNTER_KEY)
    conn.delete(READY_KEY)
    for key in conn.scan_iter(BUCKET_KEY_PATTERN):
        conn.delete(key)

    # Pin the limiter to the SAME Redis the counter uses so the single Redis
    # reachability check above is both necessary and sufficient. We deliberately do
    # NOT touch broker_url/result_backend: the workers reuse whatever broker the
    # integration environment provides (only Redis is the optional dependency).
    host = os.environ.get('REDIS_HOST', 'localhost')
    port = os.environ.get('REDIS_PORT', 6379)

    saved_enable = app.conf.worker_enable_global_rate_limits
    saved_url = app.conf.global_rate_limit_url
    app.conf.worker_enable_global_rate_limits = True
    app.conf.global_rate_limit_url = f'redis://{host}:{port}'
    try:
        yield app
    finally:
        app.conf.worker_enable_global_rate_limits = saved_enable
        app.conf.global_rate_limit_url = saved_url
        conn.delete(COUNTER_KEY)
        conn.delete(READY_KEY)


# ---------------------------------------------------------------------------
# Worker-spawn + dispatch + measurement helper
# ---------------------------------------------------------------------------
def _run_rate_limited_window(app, num_workers, window=WINDOW, dispatched=DISPATCH_COUNT):
    """Start ``num_workers`` worker processes, dispatch a backlog, and measure.

    Returns ``(count, elapsed)`` where ``count`` is the aggregate number of task
    executions recorded in the shared Redis counter across all workers during the
    window, and ``elapsed`` is the empirically measured wall-clock duration of the
    window (so the throughput bound adapts to real timing).

    Each worker runs in its own ``spawn``-ed OS process. ``expires`` ensures any
    backlog left undrained at window end auto-discards instead of polluting later
    tests; the :class:`contextlib.ExitStack` guarantees every worker process is
    terminated and joined on exit.
    """
    conn = get_redis_connection()
    conn.delete(COUNTER_KEY)
    conn.delete(READY_KEY)

    broker_url = app.conf.broker_url
    backend_url = app.conf.result_backend
    limiter_url = app.conf.global_rate_limit_url
    # Each worker idles a little past the window so it never exits mid-measurement.
    run_seconds = window + 30

    ctx = multiprocessing.get_context('spawn')
    with contextlib.ExitStack() as stack:
        procs = []
        for _ in range(num_workers):
            proc = ctx.Process(
                target=_worker_process,
                args=(broker_url, backend_url, limiter_url, READY_KEY, run_seconds),
                daemon=True,
            )
            proc.start()
            procs.append(proc)
            # LIFO teardown: terminate the process, then join it.
            stack.callback(proc.join, 15)
            stack.callback(proc.terminate)

        # Wait until every worker is up and consuming.
        ready_deadline = time.monotonic() + WORKER_READY_TIMEOUT
        while (int(conn.get(READY_KEY) or 0) < num_workers
               and time.monotonic() < ready_deadline):
            time.sleep(0.2)

        # Fail loudly if the readiness barrier timed out before every worker came
        # online. Without this guard the measurement could proceed with fewer than
        # ``num_workers`` consumers, so the N>=2 aggregate-enforcement proof would be
        # vacuous (one active worker trivially stays under the global bound). The
        # diagnostic reports each process's liveness and exit code so a worker that
        # died during startup is immediately visible.
        ready = int(conn.get(READY_KEY) or 0)
        assert ready >= num_workers, (
            f'only {ready}/{num_workers} workers signaled readiness within '
            f'{WORKER_READY_TIMEOUT}s; worker (pid, alive, exitcode)='
            f'{[(p.pid, p.is_alive(), p.exitcode) for p in procs]}'
        )
        # Brief settle so the consumers are actively polling before we dispatch.
        time.sleep(0.5)

        t0 = time.monotonic()
        for _ in range(dispatched):
            global_ratelimited_count.apply_async(expires=window + 30)
        time.sleep(window)
        elapsed = time.monotonic() - t0
        count = int(conn.get(COUNTER_KEY) or 0)
    return count, elapsed


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@flaky
def test_global_rate_limit_enforced_across_multiple_workers(global_rate_limit_app):
    """N >= 2 workers share a single global budget: aggregate is ~R, not N*R."""
    app = global_rate_limit_app
    num_workers = 2
    count, elapsed = _run_rate_limited_window(app, num_workers=num_workers)

    max_allowed = _max_allowed(elapsed)
    assert count > 0, 'workers executed nothing - check broker/worker startup'
    assert count <= max_allowed, (
        f'global limit exceeded: executed {count} in {elapsed:.2f}s '
        f'(allowed ~{max_allowed:.1f} at R={RATE_PER_SECOND}/s across {num_workers} workers)'
    )
    # Explicit non-scaling check: the aggregate must sit clearly below the
    # per-worker aggregate (~N*R*T) that independent buckets would have allowed.
    per_worker_aggregate = num_workers_aggregate(RATE_PER_SECOND, elapsed, num_workers)
    assert count < per_worker_aggregate, (
        f'throughput scaled with worker count: executed {count} in {elapsed:.2f}s '
        f'(per-worker buckets would allow ~{per_worker_aggregate:.1f})'
    )


@flaky
def test_global_rate_limit_single_worker_parity(global_rate_limit_app):
    """N = 1 worker: enabling the global limiter preserves single-worker behavior (~R)."""
    app = global_rate_limit_app
    count, elapsed = _run_rate_limited_window(app, num_workers=1)

    max_allowed = _max_allowed(elapsed)
    min_expected = _min_expected(elapsed)
    assert count > 0, 'worker executed nothing - check broker/worker startup'
    assert count <= max_allowed, (
        f'single-worker rate limit exceeded: executed {count} in {elapsed:.2f}s '
        f'(allowed ~{max_allowed:.1f} at R={RATE_PER_SECOND}/s)'
    )
    # Parity lower bound: with the global limiter enabled a single worker must
    # still sustain throughput near R (matching the local per-worker TokenBucket),
    # not merely stay under the upper bound. This is the real parity proof — it
    # catches a limiter that severely under-throttles or barely executes anything.
    assert count >= min_expected, (
        f'single-worker throughput too low for parity: executed {count} in '
        f'{elapsed:.2f}s (expected >= {min_expected:.1f} ~ R after stabilization, '
        f'R={RATE_PER_SECOND}/s); enabling the limiter must preserve ~R, like the '
        f'local per-worker bucket'
    )
