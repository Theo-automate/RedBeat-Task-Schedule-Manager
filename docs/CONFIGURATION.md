## Configuration

This package supports multiple ways to configure Redis connections:

Order of precedence:
1. Explicit arguments to `connect_to_redis_using_celery_app()`
2. `REDBEAT_*` environment variables (when `redbeat_mode=True`)
3. Celery app config (`result_backend`, `result_backend_use_ssl`, `result_backend_transport_options`), or `redbeat_*` overrides
4. General `REDIS_*` environment variables
5. Fallback to `redis://localhost:6379/0`

You can use a `.env` file locally; it will be loaded automatically when `python-dotenv` is installed.

### Direct Redis URL
- `REDIS_URL`: e.g. `redis://:password@host:6379/0`
- `REDIS_USE_SSL`: `true|false`
- `REDIS_PASSWORD`: password for non-URL cases
- `REDIS_DB`: optional DB index if not present in URL
- `REDIS_SOCKET_TIMEOUT`: seconds (float)

### Redis Sentinel
- `REDIS_SENTINELS`: `host1:26379,host2:26379`
- `REDIS_SENTINEL_SERVICE_NAME`: e.g. `mymaster`
- `REDIS_SENTINEL_PASSWORD`: optional sentinel auth
- `REDIS_PASSWORD`: password for the master

### RedBeat-specific overrides (when `redbeat_mode=True`)
- `REDBEAT_REDIS_URL`
- `REDBEAT_REDIS_USE_SSL`
- `REDBEAT_REDIS_SENTINELS`
- `REDBEAT_REDIS_SENTINEL_SERVICE_NAME`
- `REDBEAT_REDIS_SENTINEL_PASSWORD`
- `REDBEAT_REDIS_PASSWORD`
- `REDBEAT_REDIS_SOCKET_TIMEOUT`

### Celery configuration (optional)
If you pass a Celery app, we read:
- `result_backend`
- `result_backend_use_ssl`
- `result_backend_transport_options`
And for RedBeat:
- `redbeat_redis_url`, `redbeat_redis_use_ssl`, `redbeat_redis_options`

### Using .env
Install `python-dotenv` (already a dependency) and create a `.env` file:

```env
REDIS_URL=redis://:password@localhost:6379/0
# Or for Sentinel
# REDIS_SENTINELS=host1:26379,host2:26379
# REDIS_SENTINEL_SERVICE_NAME=mymaster
# REDIS_PASSWORD=master-password
```

See the docstrings in `redbeat_schedule_manager.redis_utils` for details.