"""Unit tests for the Redis-backed global (cross-worker) rate limiter.

These tests mock the Redis client with :mod:`unittest.mock` (the existing
worker-test convention) so they run without a live Redis server. The URL
parsing tests require ``redis-py`` (available via the ``celery[redis]``
extra) and are skipped when it is absent, mirroring the module's own guarded
import.
"""
import logging
from collections import deque
from unittest.mock import Mock

import pytest
from kombu.utils.limits import TokenBucket

from celery import Celery
from celery.utils.time import rate
from celery.worker import global_ratelimit as gr
from celery.worker.global_ratelimit import KEY_PREFIX, GlobalRateLimiter, GlobalTokenBucket

try:
    import redis.connection
    from redis import CredentialProvider
    redis_available = True
except ImportError:  # pragma: no cover
    redis = None
    CredentialProvider = None
    redis_available = False

requires_redis = pytest.mark.skipif(
    not redis_available,
    reason='redis-py is required for Redis URL parsing tests',
)


class RedisError(Exception):
    """Stand-in Redis connection error used to drive fail-open paths."""


def make_app(**conf):
    app = Celery(set_as_current=False)
    for key, value in conf.items():
        setattr(app.conf, key, value)
    return app


def make_bucket(**kwargs):
    kwargs.setdefault('capacity', 1)
    kwargs.setdefault('logger', Mock())
    return GlobalTokenBucket(kwargs.pop('fill_rate', 10.0), **kwargs)


class test_GlobalTokenBucket_contract:
    """The bucket must mirror the kombu TokenBucket surface consumed by the
    consumer deferral loop and shutdown path."""

    def test_contents_is_deque(self):
        assert isinstance(make_bucket().contents, deque)

    def test_add_and_pop_are_fifo(self):
        bucket = make_bucket()
        bucket.add(('r1', 1))
        bucket.add(('r2', 1))
        assert bucket.pop() == ('r1', 1)
        assert bucket.pop() == ('r2', 1)

    def test_pop_empty_raises_index_error(self):
        with pytest.raises(IndexError):
            make_bucket().pop()

    def test_clear_pending_empties_contents(self):
        bucket = make_bucket()
        bucket.add(('r', 1))
        bucket.clear_pending()
        assert len(bucket.contents) == 0

    def test_bucket_is_truthy_even_when_empty(self):
        # strategy.py and on_close test ``if bucket:`` so the bucket must be
        # truthy regardless of contents (no __len__/__bool__).
        assert bool(make_bucket()) is True

    def test_can_consume_returns_bool(self):
        assert isinstance(make_bucket().can_consume(1), bool)

    def test_expected_time_returns_float(self):
        assert isinstance(make_bucket().expected_time(1), float)

    def test_fill_rate_and_capacity_attributes(self):
        bucket = make_bucket(fill_rate=10.0, capacity=1)
        assert bucket.fill_rate == 10.0
        assert bucket.capacity == 1.0


class test_rate_units_and_namespacing:
    """Reuse Celery's rate() parser and namespace keys per task and rate."""

    def test_rate_units_via_parser(self):
        assert make_bucket(fill_rate=rate('10/s')).fill_rate == 10.0
        assert make_bucket(fill_rate=rate('100/m')).fill_rate == \
            pytest.approx(100 / 60)
        assert make_bucket(fill_rate=rate('2/h')).fill_rate == \
            pytest.approx(2 / 3600)
        assert make_bucket(fill_rate=rate(5)).fill_rate == 5.0

    def test_float_rate_is_idempotent_through_bucket(self):
        # A non-string float rate is idempotent through rate() (rate(2.5)
        # == 2.5) and is preserved unchanged along the bucket's fill_rate
        # path, even though the bucket re-applies rate() in __init__.
        assert rate(2.5) == 2.5
        assert make_bucket(fill_rate=rate(2.5)).fill_rate == 2.5
        assert make_bucket(fill_rate=rate(0.5)).fill_rate == 0.5

    def test_bucket_key_format(self):
        manager = GlobalRateLimiter(make_app())
        assert manager._bucket_key('proj.add', 10.0) == \
            f'{KEY_PREFIX}:proj.add:10.0'

    def test_distinct_rates_yield_distinct_keys(self):
        manager = GlobalRateLimiter(make_app())
        assert manager._bucket_key('proj.add', rate('10/s')) != \
            manager._bucket_key('proj.add', rate('10/m'))

    def test_distinct_tasks_yield_distinct_keys(self):
        manager = GlobalRateLimiter(make_app())
        assert manager._bucket_key('a', 10.0) != manager._bucket_key('b', 10.0)


class test_url_resolution:
    """URL resolution order: global_rate_limit_url -> result_backend ->
    broker_url."""

    def test_prefers_global_rate_limit_url(self):
        app = make_app(global_rate_limit_url='redis://g/0',
                       result_backend='redis://r/1',
                       broker_url='redis://b/2')
        assert GlobalRateLimiter(app)._resolve_url() == 'redis://g/0'

    def test_falls_back_to_result_backend(self):
        app = make_app(global_rate_limit_url=None,
                       result_backend='redis://r/1',
                       broker_url='redis://b/2')
        assert GlobalRateLimiter(app)._resolve_url() == 'redis://r/1'

    def test_falls_back_to_broker_url(self):
        app = make_app(global_rate_limit_url=None,
                       result_backend=None,
                       broker_url='redis://b/2')
        assert GlobalRateLimiter(app)._resolve_url() == 'redis://b/2'

    @requires_redis
    def test_is_redis_url(self):
        # The positive scheme assertions require redis-py to be installed:
        # when it is absent the module deliberately treats every URL as a
        # non-Redis endpoint, so the limiter fails open to local buckets
        # (that redis-absent behavior is asserted in the test below).
        manager = GlobalRateLimiter(make_app())
        assert manager._is_redis_url('redis://h/0')
        assert manager._is_redis_url('rediss://h/0')
        assert manager._is_redis_url('redis+socket:///x.sock')
        assert not manager._is_redis_url('amqp://h//')
        assert not manager._is_redis_url(None)
        assert not manager._is_redis_url('')

    def test_is_redis_url_false_when_redis_absent(self, monkeypatch):
        monkeypatch.setattr(gr, 'redis', None)
        assert not GlobalRateLimiter(make_app())._is_redis_url('redis://h/0')


@requires_redis
class test_params_from_url:
    """F2: parsing must mirror celery/backends/redis.py for the Redis URL
    features the result backend supports."""

    def _params(self, url):
        return GlobalRateLimiter(make_app())._params_from_url(url)

    def test_db_from_path(self):
        assert self._params('redis://h:6379/3')['db'] == 3

    def test_db_defaults_to_zero(self):
        assert self._params('redis://h:6379')['db'] == 0

    def test_socket_scheme(self):
        params = self._params('socket:///tmp/r.sock')
        assert params['connection_class'] is \
            redis.connection.UnixDomainSocketConnection
        assert params['path'] == '/tmp/r.sock'
        assert 'host' not in params and 'port' not in params

    def test_redis_socket_scheme_with_virtual_host_db(self):
        params = self._params('redis+socket:///tmp/r.sock?virtual_host=2')
        assert params['connection_class'] is \
            redis.connection.UnixDomainSocketConnection
        assert params['db'] == 2

    def test_socket_virtual_host_strips_leading_slash(self):
        assert self._params('socket:///tmp/r.sock?virtual_host=/3')['db'] == 3

    def test_rediss_ssl_params_are_decoded(self):
        url = ('rediss://h:6379/1?ssl_cert_reqs=required'
               '&ssl_ca_certs=%2Fp%2Fca.crt&ssl_certfile=%2Fp%2Fc.crt'
               '&ssl_keyfile=%2Fp%2Fk.key')
        params = self._params(url)
        assert params['connection_class'] is redis.SSLConnection
        assert params['ssl_ca_certs'] == '/p/ca.crt'
        assert params['ssl_certfile'] == '/p/c.crt'
        assert params['ssl_keyfile'] == '/p/k.key'
        assert params['ssl_cert_reqs'] == 'required'
        assert params['db'] == 1

    def test_redis_scheme_with_ssl_params_raises(self):
        with pytest.raises(ValueError):
            self._params('redis://h:6379/0?ssl_cert_reqs=required')

    def test_credential_provider_set_and_credentials_dropped(self):
        params = self._params(
            'redis://:pw@h:6379/1'
            '?credential_provider=redis.CredentialProvider')
        assert isinstance(params['credential_provider'], CredentialProvider)
        assert 'username' not in params
        assert 'password' not in params

    def test_invalid_credential_provider_raises(self):
        with pytest.raises(ValueError):
            self._params('redis://h:6379/0?credential_provider=abc.ABC')

    def test_query_argument_parsers_applied(self):
        params = self._params(
            'redis://h:6379/0?socket_timeout=30&health_check_interval=10')
        assert params['socket_timeout'] == 30.0
        assert isinstance(params['socket_timeout'], float)
        assert params['health_check_interval'] == 10
        assert isinstance(params['health_check_interval'], int)


class test_fail_open_parity:
    """F1: a Redis-error fallback must behave like the local TokenBucket,
    including expected_time() — the stale Redis retry hint must not leak."""

    def test_expected_time_uses_local_after_redis_error(self):
        # First call reaches Redis and is denied with a long retry hint.
        script = Mock(return_value=(0, '5.0'))
        bucket = make_bucket(
            manager=Mock(), key='k', task_name='t', script=script,
            redis_errors=(RedisError,),
        )
        assert bucket.can_consume(1) is False
        assert bucket.expected_time(1) == 5.0  # Redis value cached

        # Swap in a controllable local bucket to prove delegation.
        bucket._local = Mock()
        bucket._local.can_consume.return_value = False
        bucket._local.expected_time.return_value = 0.25

        # Second call errors -> fallback. expected_time MUST be local now,
        # NOT the stale 5.0 cached from the previous Redis denial.
        script.side_effect = RedisError('down')
        assert bucket.can_consume(1) is False
        assert bucket.expected_time(1) == 0.25
        bucket._local.expected_time.assert_called_once_with(1)

    def test_redis_error_warns_with_task_and_key_and_counts(self, caplog):
        # Drive the Redis-error fail-open path through the REAL module logger
        # and a REAL manager so the actual WARNING record is captured by
        # ``caplog`` and the real ``fallback_count`` counter is asserted
        # (not just a mock's call args).
        manager = GlobalRateLimiter(make_app())
        script = Mock(side_effect=RedisError('down'))
        bucket = make_bucket(
            manager=manager, key='celery:globalratelimit:proj.t:10.0',
            task_name='proj.t', script=script, redis_errors=(RedisError,),
            logger=None,
        )
        with caplog.at_level(logging.WARNING,
                             logger='celery.worker.global_ratelimit'):
            assert isinstance(bucket.can_consume(1), bool)
        records = [r for r in caplog.records
                   if r.name == 'celery.worker.global_ratelimit'
                   and r.levelno == logging.WARNING]
        assert len(records) == 1
        message = records[0].getMessage()
        assert 'proj.t' in message
        assert 'celery:globalratelimit:proj.t:10.0' in message
        assert manager.fallback_count == 1

    def test_no_exception_escapes_can_consume(self):
        # A non-Redis error must still be caught (fail-open is absolute).
        script = Mock(side_effect=ValueError('unexpected'))
        bucket = make_bucket(
            manager=Mock(), key='k', task_name='t', script=script,
            redis_errors=(RedisError,),
        )
        assert isinstance(bucket.can_consume(1), bool)

    def test_fallback_tracks_local_token_bucket(self):
        # Permanent fallback (no script) must mirror a real TokenBucket: a
        # capacity-1 bucket allows the first token then denies the immediate
        # second.
        bucket = make_bucket(
            manager=Mock(), key='k', task_name='t', script=None,
        )
        reference = TokenBucket(10.0, capacity=1)
        assert bucket.can_consume(1) == reference.can_consume(1)
        assert bucket.can_consume(1) == reference.can_consume(1)


class test_permanent_fallback_warning:
    """F3: permanent fail-open modes must emit one WARNING with the task name
    and bucket key, and keep incrementing the fallback counter."""

    def test_script_none_warns_once_with_task_and_key(self):
        manager = Mock()
        logger = Mock()
        bucket = make_bucket(
            manager=manager, key='celery:globalratelimit:proj.t:10.0',
            task_name='proj.t', script=None, logger=logger,
        )
        bucket.can_consume(1)
        bucket.can_consume(1)
        bucket.can_consume(1)
        logger.warning.assert_called_once()  # log-once, not per call
        args = logger.warning.call_args.args
        assert 'proj.t' in args
        assert 'celery:globalratelimit:proj.t:10.0' in args
        assert manager._incr_fallback.call_count == 3  # counted every call

    def test_redis_absent_reason(self, monkeypatch):
        monkeypatch.setattr(gr, 'redis', None)
        logger = Mock()
        bucket = make_bucket(
            manager=Mock(), key='k', task_name='t', script=None, logger=logger,
        )
        bucket.can_consume(1)
        reason = logger.warning.call_args.args[3]
        assert reason == 'redis-py is not installed'

    def test_non_redis_url_via_manager_warns_and_counts(self):
        app = make_app(global_rate_limit_url='memory://',
                       result_backend=None, broker_url='memory://')
        manager = GlobalRateLimiter(app)
        manager._logger = Mock()
        bucket = manager.bucket('proj.t', 1.0)
        assert manager.script is None  # non-Redis URL -> no script
        bucket.can_consume(1)
        bucket.can_consume(1)
        manager._logger.warning.assert_called_once()
        args = manager._logger.warning.call_args.args
        assert 'proj.t' in args
        assert manager._bucket_key('proj.t', 1.0) in args
        assert manager.fallback_count == 2


class test_GlobalRateLimiter_close:
    """F4: close() must swallow best-effort cleanup errors (no bare pass) and
    never raise."""

    def test_close_swallows_errors_and_logs_debug(self):
        manager = GlobalRateLimiter(make_app())
        manager._logger = Mock()
        client = Mock()
        client.close.side_effect = RuntimeError('boom')
        client.connection_pool.disconnect.side_effect = RuntimeError('boom2')
        manager._client = client
        manager._client_initialized = True
        manager._script = Mock()

        manager.close()  # must not raise

        assert manager._client is None
        assert manager._script is None
        assert manager._client_initialized is False
        assert manager._logger.debug.call_count == 2

    def test_close_without_client_is_noop(self):
        manager = GlobalRateLimiter(make_app())
        manager.close()  # no client created; must not raise
        assert manager._client is None
        assert manager._client_initialized is False


class test_GlobalRateLimiter_counters:
    """allow/deny/fallback counters aggregate per-bucket outcomes."""

    def test_allow_and_deny_counted(self):
        manager = GlobalRateLimiter(make_app())
        script = Mock()
        bucket = GlobalTokenBucket(
            10.0, capacity=1, manager=manager, key='k', task_name='t',
            script=script, redis_errors=(RedisError,), logger=Mock(),
        )
        script.return_value = (1, '0')
        assert bucket.can_consume(1) is True
        script.return_value = (0, '0.5')
        assert bucket.can_consume(1) is False
        assert manager.allow_count == 1
        assert manager.deny_count == 1
        assert manager.fallback_count == 0


class test_atomic_acquisition:
    """Group 3: token acquisition is a SINGLE atomic Redis call carrying the
    namespaced bucket key and the ``[fill_rate, capacity, tokens, ttl]``
    arguments -- proving the limiter performs exactly one server-side
    operation per acquisition attempt (no client-side read-modify-write race
    across workers)."""

    def test_single_atomic_call_uses_namespaced_key_and_args(self):
        manager = GlobalRateLimiter(make_app())
        # The registered Lua script returns ``[allowed, retry_after_str]``;
        # allow the first token then deny the immediate second.
        script = Mock(side_effect=[[1, '0'], [0, '0.5']])
        key = manager._bucket_key('proj.add', 10.0)
        bucket = make_bucket(
            fill_rate=10.0, capacity=1, manager=manager, key=key,
            task_name='proj.add', script=script, redis_errors=(),
        )

        assert bucket.can_consume(1) is True   # first token granted
        assert bucket.can_consume(1) is False  # bucket empty -> denied

        # Exactly one server-side invocation per acquisition attempt.
        assert script.call_count == 2
        first = script.call_args_list[0]
        assert first.kwargs['keys'] == [key]
        args = first.kwargs['args']
        # Positional contract: [fill_rate, capacity, tokens, ttl]. Assert the
        # first three exactly and the ttl only as a positive bound so the test
        # stays robust to any valid change in the ttl formula.
        assert args[0] == 10.0 and args[1] == 1.0 and args[2] == 1
        assert len(args) == 4 and args[3] > 0

        # The retry_after string from the deny is float()-converted and cached
        # so expected_time() reports the Redis-reported wait.
        assert bucket.expected_time(1) == 0.5
        assert isinstance(bucket.expected_time(1), float)
        assert manager.allow_count == 1 and manager.deny_count == 1
