"""Redis-backed global (cross-worker) task rate limiting."""
from collections import deque

from kombu.utils.limits import TokenBucket
from kombu.utils.url import _parse_url, maybe_sanitize_url

from celery.utils.log import get_logger
from celery.utils.time import rate

try:
    import redis.connection
    from kombu.transport.redis import get_redis_error_classes
except ImportError:
    redis = None
    get_redis_error_classes = None

__all__ = ('GlobalRateLimiter', 'GlobalTokenBucket', 'KEY_PREFIX')

logger = get_logger(__name__)

#: Namespace prefix for every global rate-limit bucket key in Redis.
KEY_PREFIX = 'celery:globalratelimit'

#: Schemes accepted as a usable Redis endpoint for the limiter.
REDIS_SCHEMES = frozenset({'redis', 'rediss', 'redis+socket', 'socket'})

#: Atomic token-bucket acquisition executed server-side in a single round
#: trip. Reads the Redis server clock so every worker shares one time source,
#: refills the bucket from elapsed time, conditionally consumes a token, bounds
#: key growth with a TTL, and returns ``{allowed, retry_after}``.
LUA_TOKEN_ACQUIRE = """
-- read the server clock (seconds.microseconds) so workers share one time base
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
-- load current bucket state
local data = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens = tonumber(data[1])
local last = tonumber(data[2])
local fill_rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
if tokens == nil then tokens = capacity end
if last == nil then last = now end
-- refill based on elapsed time, clamped to capacity
local elapsed = now - last
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * fill_rate)
-- conditionally consume the requested tokens
local allowed = 0
local retry_after = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    retry_after = (requested - tokens) / fill_rate
end
-- persist new state and bound key growth
redis.call('HSET', KEYS[1], 'tokens', tokens, 'last', now)
if ttl > 0 then
    redis.call('EXPIRE', KEYS[1], ttl)
end
return {allowed, tostring(retry_after)}
"""


class GlobalRateLimiter:
    """Owns the shared Redis client, the registered acquisition script,
    metric counters, and the per-task bucket factory.

    A single instance is created lazily by the worker consumer and reused for
    every task and across every :meth:`reset_rate_limits` rebuild. When Redis
    is unavailable (or ``redis-py`` is not installed, or the resolved URL is
    not a Redis endpoint) the limiter operates in permanent fail-open mode and
    every bucket it produces falls back to a local per-worker token bucket.
    """

    def __init__(self, app):
        self.app = app
        self._logger = logger
        self._client = None
        self._script = None
        self._client_initialized = False
        self.allow_count = 0
        self.deny_count = 0
        self.fallback_count = 0

    def _resolve_url(self):
        conf = self.app.conf
        return (conf.global_rate_limit_url
                or conf.result_backend
                or conf.broker_url)

    def _is_redis_url(self, url):
        if redis is None or not url or not isinstance(url, str):
            return False
        scheme = url.split('://', 1)[0].lower() if '://' in url else ''
        return scheme in REDIS_SCHEMES

    def _params_from_url(self, url):
        scheme, host, port, username, password, path, query = _parse_url(url)
        connparams = {
            key: value
            for key, value in {
                'host': host,
                'port': port,
                'username': username,
                'password': password,
            }.items()
            if value is not None
        }
        if scheme in ('socket', 'redis+socket'):
            connparams['connection_class'] = \
                redis.connection.UnixDomainSocketConnection
            connparams['path'] = '/' + path if path else path
            connparams.pop('host', None)
            connparams.pop('port', None)
            db = query.get('virtual_host')
        else:
            db = path
            if scheme == 'rediss':
                connparams['connection_class'] = redis.SSLConnection
        db = db or 0
        if isinstance(db, str):
            db = db.strip('/')
        try:
            connparams['db'] = int(db)
        except (TypeError, ValueError):
            connparams['db'] = 0
        return connparams

    @property
    def client(self):
        if self._client_initialized:
            return self._client
        self._client_initialized = True
        url = None
        try:
            url = self._resolve_url()
            if not self._is_redis_url(url):
                self._client = None
                return None
            connparams = self._params_from_url(url)
            self._client = redis.StrictRedis(
                connection_pool=redis.ConnectionPool(**connparams))
        except Exception as exc:
            self._logger.warning(
                'Global rate limiter could not initialize Redis client '
                'for %s: %r; falling back to local per-worker buckets.',
                maybe_sanitize_url(url), exc)
            self._client = None
        return self._client

    @property
    def script(self):
        if self._script is not None:
            return self._script
        client = self.client
        if client is None:
            return None
        try:
            self._script = client.register_script(LUA_TOKEN_ACQUIRE)
        except Exception as exc:
            self._logger.warning(
                'Global rate limiter could not register Lua script: %r; '
                'falling back to local per-worker buckets.', exc)
            self._script = None
        return self._script

    def ping(self):
        client = self.client
        if client is None:
            return False
        try:
            return bool(client.ping())
        except Exception:
            return False

    def redis_errors(self):
        if get_redis_error_classes is None:
            return ()
        connection_errors, channel_errors = get_redis_error_classes()
        return tuple(connection_errors) + tuple(channel_errors)

    def _incr_allow(self):
        self.allow_count += 1

    def _incr_deny(self):
        self.deny_count += 1

    def _incr_fallback(self):
        self.fallback_count += 1

    def _bucket_key(self, task_name, limit):
        return f'{KEY_PREFIX}:{task_name}:{limit}'

    def bucket(self, task_name, limit):
        return GlobalTokenBucket(
            limit,
            capacity=1,
            manager=self,
            key=self._bucket_key(task_name, limit),
            task_name=task_name,
            script=self.script,
            redis_errors=self.redis_errors(),
            logger=self._logger,
        )

    def close(self):
        client = self._client
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
            try:
                client.connection_pool.disconnect()
            except Exception:
                pass
        self._client = None
        self._script = None
        self._client_initialized = False


class GlobalTokenBucket:
    """Redis-backed bucket mirroring :class:`kombu.utils.limits.TokenBucket`.

    The consumer deferral loop interacts with this object exactly as it does
    with the per-worker :class:`~kombu.utils.limits.TokenBucket`: it keeps an
    in-process :class:`~collections.deque` of pending requests
    (``add``/``pop``/``clear_pending`` plus direct ``contents`` access) while
    :meth:`can_consume` performs the atomic, cross-worker token acquisition in
    Redis and :meth:`expected_time` reports the wait returned by that script.

    On any Redis error the bucket logs a warning and delegates to an internal
    local :class:`~kombu.utils.limits.TokenBucket`, guaranteeing fail-open
    parity with the existing per-worker behavior. No exception ever escapes
    :meth:`can_consume` or :meth:`expected_time`.
    """

    def __init__(self, fill_rate, capacity=1, *, manager=None, key=None,
                 task_name=None, script=None, redis_errors=None, logger=None):
        self.fill_rate = rate(fill_rate)
        self.capacity = float(capacity)
        self.contents = deque()
        self._manager = manager
        self._key = key
        self._task_name = task_name
        self._script = script
        self._logger = logger or get_logger(__name__)
        self._redis_errors = tuple(redis_errors) if redis_errors else ()
        self._retry_after = None
        local_fill_rate = self.fill_rate if self.fill_rate > 0 else 1.0
        self._local = TokenBucket(local_fill_rate, capacity=1)
        self._fallback_only = script is None or self.fill_rate <= 0
        if self.fill_rate > 0:
            self._ttl = max(int(self.capacity / self.fill_rate) + 1, 60)
        else:
            self._ttl = 60

    def add(self, item):
        self.contents.append(item)

    def pop(self):
        return self.contents.popleft()

    def clear_pending(self):
        self.contents.clear()

    def can_consume(self, tokens=1):
        if self._fallback_only:
            if self._manager is not None:
                self._manager._incr_fallback()
            return self._local.can_consume(tokens)
        try:
            allowed, retry_after = self._script(
                keys=[self._key],
                args=[self.fill_rate, self.capacity, tokens, self._ttl],
            )
            self._retry_after = float(retry_after)
            if allowed:
                if self._manager is not None:
                    self._manager._incr_allow()
                return True
            if self._manager is not None:
                self._manager._incr_deny()
            return False
        except self._redis_errors as exc:
            return self._fallback(tokens, exc)
        except Exception as exc:
            return self._fallback(tokens, exc)

    def _fallback(self, tokens, exc):
        self._logger.warning(
            'Global rate limiter unavailable for task %s (key=%s): %r; '
            'falling back to local per-worker bucket.',
            self._task_name, self._key, exc,
        )
        if self._manager is not None:
            self._manager._incr_fallback()
        return self._local.can_consume(tokens)

    def expected_time(self, tokens=1):
        if self._fallback_only or self._retry_after is None:
            return self._local.expected_time(tokens)
        return float(self._retry_after)
