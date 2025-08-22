import inspect
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from redis import Redis
from redis.sentinel import Sentinel

try:
    from celery import Celery  # Optional
except Exception:  # pragma: no cover
    Celery = object  # type: ignore

try:
    from croniter import croniter  # type: ignore[import-untyped]
except Exception as import_error:  # pragma: no cover
    raise RuntimeError("croniter is required. Please install with: pip install croniter") from import_error

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # dotenv is optional; if not present, we rely on environment already loaded
    pass


LOGGER = logging.getLogger(__name__)
CALLER_SCRIPT = os.path.basename(inspect.stack()[-1].filename)
RedisOrSentinel = Redis


def _coerce_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    lowered = value.strip().lower()
    return lowered in {"1", "true", "yes", "y", "on"}


def _parse_sentinel_csv(csv: str) -> list[tuple[str, int]]:
    pairs: list[tuple[str, int]] = []
    for part in csv.split(","):
        part = part.strip()
        if not part:
            continue
        host, _, port_str = part.partition(":")
        try:
            port = int(port_str or "26379")
        except ValueError:
            port = 26379
        pairs.append((host, port))
    return pairs


def calculate_next_run_time(cron_expression: str, *, base_utc: Optional[datetime] = None) -> int:
    """Return the next run time (UTC seconds) for the given cron expression.

    - Uses croniter and current UTC as base by default
    - Returns integer epoch seconds
    """
    base = base_utc or datetime.now(timezone.utc)
    iterator = croniter(cron_expression, base)
    next_dt = iterator.get_next(datetime)
    return int(next_dt.timestamp())


def connect_to_redis_using_celery_app(
    celery_app: Optional[Celery] = None,
    *,
    redbeat_mode: bool = False,
    url: Optional[str] = None,
    use_ssl: Optional[bool] = None,
    options: Optional[dict[str, Any]] = None,
) -> RedisOrSentinel:
    """Create a Redis connection from multiple configuration sources.

    Configuration resolution order (highest precedence first):
    1) Explicit function arguments: url/use_ssl/options
    2) REDBEAT_* environment variables (when redbeat_mode=True)
    3) CELERY app config (result_backend and related fields)
    4) General REDIS_* environment variables
    5) Fallback to redis://localhost:6379/0

    Environment variables supported:
    - REDIS_URL (e.g. redis://user:pass@host:6379/0)
    - REDIS_USE_SSL (true/false)
    - REDIS_PASSWORD
    - REDIS_DB (integer)
    - REDIS_SENTINELS (comma list host:port,...)
    - REDIS_SENTINEL_SERVICE_NAME
    - REDIS_SENTINEL_PASSWORD
    - REDIS_SOCKET_TIMEOUT (seconds)
    - REDBEAT_REDIS_URL, REDBEAT_REDIS_USE_SSL, REDBEAT_REDIS_SENTINELS, ...
      (override when redbeat_mode=True)

    If a Sentinel configuration is provided, the connection will be established
    to the master.
    """
    resolved_url: str
    resolved_use_ssl: bool
    resolved_options: dict[str, Any] = {}

    # 1) Explicit args
    if url:
        resolved_url = url
    else:
        resolved_url = ""
    if use_ssl is not None:
        resolved_use_ssl = use_ssl
    else:
        resolved_use_ssl = False
    if options:
        resolved_options.update(options)

    # 2) REDBEAT_* env overrides (when redbeat_mode)
    if redbeat_mode:
        resolved_url = os.getenv("REDBEAT_REDIS_URL", resolved_url)
        if "REDBEAT_REDIS_USE_SSL" in os.environ:
            resolved_use_ssl = _coerce_bool(os.getenv("REDBEAT_REDIS_USE_SSL"))
        # Sentinel via env
        if "REDBEAT_REDIS_SENTINELS" in os.environ:
            resolved_options["sentinels"] = _parse_sentinel_csv(os.environ["REDBEAT_REDIS_SENTINELS"])  # type: ignore[index]
        service_name = os.getenv("REDBEAT_REDIS_SENTINEL_SERVICE_NAME")
        if service_name:
            resolved_options["service_name"] = service_name
        # passwords
        if os.getenv("REDBEAT_REDIS_PASSWORD"):
            resolved_options["password"] = os.environ["REDBEAT_REDIS_PASSWORD"]
        if os.getenv("REDBEAT_REDIS_SENTINEL_PASSWORD"):
            resolved_options["sentinel_password"] = os.environ["REDBEAT_REDIS_SENTINEL_PASSWORD"]
        # socket timeout
        if os.getenv("REDBEAT_REDIS_SOCKET_TIMEOUT"):
            try:
                resolved_options["socket_timeout"] = float(os.environ["REDBEAT_REDIS_SOCKET_TIMEOUT"])  # type: ignore[index]
            except Exception:
                pass

    # 3) Celery app config
    if celery_app is not None and hasattr(celery_app, "conf"):
        backend_url = getattr(celery_app.conf, "result_backend", None) or celery_app.conf.get("result_backend")
        backend_use_ssl = celery_app.conf.get("result_backend_use_ssl", None)
        backend_options = celery_app.conf.get("result_backend_transport_options", {}) or {}
        if not resolved_url and isinstance(backend_url, str):
            resolved_url = backend_url
        if resolved_use_ssl is False and isinstance(backend_use_ssl, bool):
            resolved_use_ssl = backend_use_ssl
        # RedBeat-specific celery overrides
        if redbeat_mode:
            resolved_url = celery_app.conf.get("redbeat_redis_url", resolved_url)
            resolved_use_ssl = celery_app.conf.get("redbeat_redis_use_ssl", resolved_use_ssl)
            backend_options.update(celery_app.conf.get("redbeat_redis_options", {}))
        resolved_options.update(backend_options)

    # 4) General REDIS_* env
    if not resolved_url:
        resolved_url = os.getenv("REDIS_URL", "")
    if "REDIS_USE_SSL" in os.environ and use_ssl is None:
        resolved_use_ssl = _coerce_bool(os.getenv("REDIS_USE_SSL"))
    if "REDIS_SENTINELS" in os.environ and "sentinels" not in resolved_options:
        resolved_options["sentinels"] = _parse_sentinel_csv(os.environ["REDIS_SENTINELS"])  # type: ignore[index]
    service_name = os.getenv("REDIS_SENTINEL_SERVICE_NAME")
    if service_name and "service_name" not in resolved_options:
        resolved_options["service_name"] = service_name
    if os.getenv("REDIS_PASSWORD") and "password" not in resolved_options:
        resolved_options["password"] = os.environ["REDIS_PASSWORD"]
    if os.getenv("REDIS_SENTINEL_PASSWORD") and "sentinel_password" not in resolved_options:
        resolved_options["sentinel_password"] = os.environ["REDIS_SENTINEL_PASSWORD"]
    if os.getenv("REDIS_SOCKET_TIMEOUT") and "socket_timeout" not in resolved_options:
        try:
            resolved_options["socket_timeout"] = float(os.environ["REDIS_SOCKET_TIMEOUT"])  # type: ignore[index]
        except Exception:
            pass

    # 5) Fallback
    if not resolved_url:
        resolved_url = "redis://localhost:6379/0"

    # Build connection
    sentinels: Optional[list[tuple[str, int]]] = resolved_options.get("sentinels")  # type: ignore[assignment]
    if sentinels:
        # Sentinel connection
        password = resolved_options.get("password")
        sentinel_password = resolved_options.get("sentinel_password")
        service = resolved_options.get("service_name") or "mymaster"
        socket_timeout = resolved_options.get("socket_timeout", 5)
        sentinel_client = Sentinel(
            sentinels,
            sentinel_kwargs={"socket_timeout": socket_timeout, "password": sentinel_password},
            password=password,
            socket_timeout=socket_timeout,
        )
        redis_master = sentinel_client.master_for(service, socket_timeout=socket_timeout, ssl=resolved_use_ssl)
        LOGGER.info("[%s] Connected to Redis via Sentinel service=%s", CALLER_SCRIPT, service)
        return redis_master

    # Direct URL: redis://[:password]@host:port/db
    db = int(resolved_url.rsplit("/", 1)[-1]) if "/" in resolved_url else 0
    host_port = resolved_url.split("://", 1)[-1]
    host_only = host_port.split(":")[0]
    port_only = host_port.rsplit(":", 1)[-1].split("/")[0]
    try:
        port = int(port_only)
    except Exception:
        port = 6379
    redis_master = Redis(
        host=host_only,
        port=port,
        db=db,
        ssl=resolved_use_ssl,
        password=resolved_options.get("password"),
    )
    LOGGER.info("[%s] Connected to Redis - %s", CALLER_SCRIPT, resolved_url)
    return redis_master


def search_redis_keys(pattern: str, redis_env: RedisOrSentinel) -> list[str]:
    keys: list[str] = []
    try:
        if not pattern or not isinstance(pattern, str):
            LOGGER.error("[%s] Invalid pattern provided", CALLER_SCRIPT)
            return keys
        keys = [key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else str(key) for key in redis_env.keys(pattern)]
    except Exception as exc:
        LOGGER.exception("[%s] Failed to search for keys in Redis: %s", CALLER_SCRIPT, exc)
        return []
    return keys


def check_redis_key_exists(key: str, redis_env: RedisOrSentinel) -> bool:
    return redis_env.exists(key) > 0


def get_redis_key_value(key: str, redis_env: RedisOrSentinel) -> Any:
    value_dict: Any = None
    try:
        if not check_redis_key_exists(key, redis_env):
            LOGGER.warning("[%s] Key %s does not exist in Redis", CALLER_SCRIPT, key)
            return value_dict
        key_type = redis_env.type(key).decode("utf-8")
        if key_type == "hash":
            encoded = redis_env.hgetall(key)
            value_dict = {}
            for k, v in encoded.items():
                k_str = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)
                v_str = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v
                try:
                    value_dict[k_str] = json.loads(v_str)
                except Exception:
                    value_dict[k_str] = v_str
        elif key_type == "string":
            raw = redis_env.get(key)
            value_dict = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        elif key_type == "set":
            members = redis_env.smembers(key)
            value_dict = [m.decode("utf-8") if isinstance(m, (bytes, bytearray)) else m for m in members]
        elif key_type == "zset":
            elements = redis_env.zrange(key, 0, -1, withscores=True)
            value_dict = [(e.decode("utf-8") if isinstance(e, (bytes, bytearray)) else e, score) for e, score in elements]
        else:
            LOGGER.error("Unknown key type: %s", key_type)
    except Exception as exc:
        LOGGER.exception("[%s] Failed to get value of key %s from Redis: %s", CALLER_SCRIPT, key, exc)
    return value_dict


def bulk_get_redis_key_values(keys: list[str], redis_env: RedisOrSentinel) -> dict[str, Any]:
    result: dict[str, Any] = {}
    try:
        if not keys or not isinstance(keys, list):
            LOGGER.warning("[%s] No keys provided to bulk_get_redis_key_values", CALLER_SCRIPT)
            return result
        pipe = redis_env.pipeline()
        for key in keys:
            pipe.exists(key)
            pipe.type(key)
        pipeline_results = pipe.execute()
        key_info: dict[str, str] = {}
        for i, key in enumerate(keys):
            exists_result = pipeline_results[i * 2]
            type_result = pipeline_results[i * 2 + 1]
            if exists_result > 0:
                key_type = type_result.decode("utf-8") if isinstance(type_result, (bytes, bytearray)) else type_result
                key_info[key] = key_type
            else:
                LOGGER.debug("[%s] Key %s does not exist in Redis", CALLER_SCRIPT, key)
        pipe = redis_env.pipeline()
        valid_keys: list[str] = []
        for key, key_type in key_info.items():
            valid_keys.append(key)
            if key_type == "hash":
                pipe.hgetall(key)
            elif key_type == "string":
                pipe.get(key)
            elif key_type == "set":
                pipe.smembers(key)
            elif key_type == "zset":
                pipe.zrange(key, 0, -1, withscores=True)
            else:
                valid_keys.pop()
        if valid_keys:
            data_results = pipe.execute()
            for i, key in enumerate(valid_keys):
                key_type = key_info[key]
                raw_value = data_results[i]
                if key_type == "hash":
                    converted: dict[str, Any] = {}
                    for k, v in (raw_value or {}).items():
                        k_str = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)
                        v_str = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v
                        try:
                            converted[k_str] = json.loads(v_str)
                        except Exception:
                            converted[k_str] = v_str
                    result[key] = converted
                elif key_type == "string":
                    result[key] = raw_value.decode("utf-8") if isinstance(raw_value, (bytes, bytearray)) else raw_value
                elif key_type == "set":
                    result[key] = [m.decode("utf-8") if isinstance(m, (bytes, bytearray)) else m for m in (raw_value or [])]
                elif key_type == "zset":
                    result[key] = [
                        (
                            e.decode("utf-8") if isinstance(e, (bytes, bytearray)) else e,
                            score,
                        )
                        for e, score in (raw_value or [])
                    ]
        LOGGER.info("[%s] Retrieved %d keys via pipeline", CALLER_SCRIPT, len(result))
    except Exception as exc:
        LOGGER.exception("[%s] Failed bulk get from Redis: %s", CALLER_SCRIPT, exc)
        result = {}
    return result


def update_redis_key(key: str, value: Any, redis_env: RedisOrSentinel) -> bool:
    """Update or create a Redis key; choose type based on existing type or value.

    - Hash: dict (fields JSON-encoded)
    - String: str/bytes/int/float
    - Set: list (members added; not clearing existing ones)
    - ZSet: tuple mapping or dict name->score for schedule zset
    - Special-case: keys ending with "::schedule" are ZSet
    """
    success = False
    try:
        if key.endswith("::schedule"):
            key_type = "zset"
        else:
            key_type = redis_env.type(key).decode("utf-8")
        if key_type == "none":
            if isinstance(value, dict):
                key_type = "hash"
            elif isinstance(value, (str, bytes, int, float)):
                key_type = "string"
            elif isinstance(value, list):
                key_type = "set"
            elif isinstance(value, tuple) or isinstance(value, dict):
                key_type = "zset"
            else:
                LOGGER.error("Unhandled value type: %s", type(value))
                return False
        if key_type == "hash":
            if not isinstance(value, dict):
                LOGGER.error("Value is not a dictionary for hash key %s", key)
                return False
            for k, v in value.items():
                redis_env.hset(key, key=k, value=json.dumps(v).encode("utf-8"))
            success = True
        elif key_type == "string":
            redis_env.set(key, value)
            success = True
        elif key_type == "set":
            redis_env.sadd(key, *value if isinstance(value, list) else [value])
            success = True
        elif key_type == "zset":
            # Accept dict name->score or list of (name, score)
            if isinstance(value, dict):
                mapping = value
            elif isinstance(value, (list, tuple)):
                mapping = dict(value)
            else:
                LOGGER.error("Unsupported zset value for %s", key)
                return False
            redis_env.zadd(key, mapping)
            success = True
        else:
            LOGGER.error("Unknown key type: %s", key_type)
            success = False
        if success:
            LOGGER.info("[%s] Updated key %s", CALLER_SCRIPT, key)
    except Exception as exc:
        LOGGER.exception("[%s] Failed to update key %s: %s", CALLER_SCRIPT, key, exc)
        success = False
    return success


def bulk_update_redis_keys(keys: dict[str, Any], redis_env: RedisOrSentinel) -> dict[str, bool]:
    success_dict: dict[str, bool] = {k: False for k in keys}
    try:
        if not keys or not isinstance(keys, dict):
            LOGGER.warning("[%s] No keys provided to bulk_update_redis_keys", CALLER_SCRIPT)
            return success_dict
        pipe = redis_env.pipeline()
        # Determine current types
        for key in keys.keys():
            pipe.type(key)
        type_results = pipe.execute()
        op_list: list[tuple[str, str, Any]] = []
        for i, key in enumerate(keys.keys()):
            value = keys[key]
            current_type = type_results[i].decode("utf-8")
            if key.endswith("::schedule"):
                target_type = "zset"
            elif current_type == "none":
                if isinstance(value, dict):
                    target_type = "hash"
                elif isinstance(value, (str, bytes, int, float)):
                    target_type = "string"
                elif isinstance(value, list):
                    target_type = "set"
                elif isinstance(value, (tuple, dict)):
                    target_type = "zset"
                else:
                    LOGGER.error("Unhandled value type: %s for %s", type(value), key)
                    continue
            else:
                target_type = current_type
            op_list.append((key, target_type, value))
        # Queue ops
        pipe = redis_env.pipeline()
        for key, key_type, value in op_list:
            if key_type == "hash":
                if not isinstance(value, dict):
                    continue
                for k, v in value.items():
                    pipe.hset(key, key=k, value=json.dumps(v).encode("utf-8"))
            elif key_type == "string":
                pipe.set(key, value)
            elif key_type == "set":
                pipe.sadd(key, *value if isinstance(value, list) else [value])
            elif key_type == "zset":
                mapping = value if isinstance(value, dict) else dict(value)
                pipe.zadd(key, mapping)
        operation_results = pipe.execute() if op_list else []
        # Interpret results (best-effort)
        idx = 0
        for key, key_type, value in op_list:
            if key_type == "hash":
                # hset returns 1/0 per field; treat presence of results as success
                field_count = len(value) if isinstance(value, dict) else 1
                success_dict[key] = all(bool(operation_results[idx + j]) or operation_results[idx + j] == 0 for j in range(field_count))
                idx += field_count
            else:
                success_dict[key] = bool(operation_results[idx]) or operation_results[idx] == 0
                idx += 1
        LOGGER.info("[%s] Bulk updated %d keys", CALLER_SCRIPT, sum(1 for v in success_dict.values() if v))
    except Exception as exc:
        LOGGER.exception("[%s] Failed bulk update in Redis: %s", CALLER_SCRIPT, exc)
        success_dict = {k: False for k in keys}
    return success_dict


def delete_redis_key(key: str, redis_env: RedisOrSentinel) -> bool:
    try:
        if not check_redis_key_exists(key, redis_env):
            LOGGER.info("Key %s does not exist; treated as deleted", key)
            return True
        redis_env.delete(key)
        LOGGER.info("[%s] Deleted key %s", CALLER_SCRIPT, key)
        return True
    except Exception as exc:
        LOGGER.exception("[%s] Failed to delete key %s: %s", CALLER_SCRIPT, key, exc)
        return False


def remove_item_from_redis_key(key: str, item: Any, redis_env: RedisOrSentinel) -> bool:
    try:
        if not check_redis_key_exists(key, redis_env):
            LOGGER.info("Key %s does not exist; nothing to remove", key)
            return True
        key_type = redis_env.type(key).decode("utf-8")
        if key_type == "hash":
            redis_env.hdel(key, item)
        elif key_type == "set":
            redis_env.srem(key, item)
        elif key_type == "zset":
            redis_env.zrem(key, item)
        else:
            LOGGER.error("Unknown key type: %s", key_type)
            return False
        LOGGER.info("[%s] Removed item %s from %s", CALLER_SCRIPT, item, key)
        return True
    except Exception as exc:
        LOGGER.exception("[%s] Failed removing item from %s: %s", CALLER_SCRIPT, key, exc)
        return False


def bulk_remove_items_from_redis_key(key: str, items: list[Any], redis_env: RedisOrSentinel) -> dict[str, bool]:
    success_dict: dict[str, bool] = {str(it): False for it in items}
    try:
        if not check_redis_key_exists(key, redis_env):
            LOGGER.info("Key %s does not exist; nothing to remove", key)
            return {str(it): True for it in items}
        key_type = redis_env.type(key).decode("utf-8")
        pipe = redis_env.pipeline()
        for item in items:
            if key_type == "hash":
                pipe.hdel(key, item)
            elif key_type == "set":
                pipe.srem(key, item)
            elif key_type == "zset":
                pipe.zrem(key, item)
            else:
                LOGGER.error("Unknown key type: %s", key_type)
                return success_dict
        results = pipe.execute()
        for i, item in enumerate(items):
            success_dict[str(item)] = bool(results[i]) or results[i] == 0
        return success_dict
    except Exception as exc:
        LOGGER.exception("[%s] Failed bulk remove from %s: %s", CALLER_SCRIPT, key, exc)
        return {str(it): False for it in items}


def bulk_delete_redis_keys(keys: list[str], redis_env: RedisOrSentinel) -> dict[str, bool]:
    success_dict: dict[str, bool] = {k: False for k in keys}
    try:
        pipe = redis_env.pipeline()
        for key in keys:
            pipe.exists(key)
        existence = pipe.execute()
        keys_to_delete: list[str] = []
        for idx, key in enumerate(keys):
            if existence[idx] > 0:
                keys_to_delete.append(key)
            else:
                success_dict[key] = True
        if keys_to_delete:
            pipe = redis_env.pipeline()
            for key in keys_to_delete:
                pipe.delete(key)
            results = pipe.execute()
            for i, key in enumerate(keys_to_delete):
                success_dict[key] = results[i] > 0
        LOGGER.info("[%s] Bulk deleted %d keys", CALLER_SCRIPT, sum(1 for v in success_dict.values() if v))
        return success_dict
    except Exception as exc:
        LOGGER.exception("[%s] Failed bulk delete in Redis: %s", CALLER_SCRIPT, exc)
        return {k: False for k in keys}


def bulk_set_redis_key_expiration(keys: list[str], expiration_seconds: int, redis_env: RedisOrSentinel) -> dict[str, bool]:
    success_dict: dict[str, bool] = {k: False for k in keys}
    try:
        if not isinstance(expiration_seconds, int):
            LOGGER.error("expiration_seconds must be an int")
            return success_dict
        pipe = redis_env.pipeline()
        for key in keys:
            pipe.expire(key, expiration_seconds)
        results = pipe.execute()
        for i, key in enumerate(keys):
            success_dict[key] = bool(results[i])
        return success_dict
    except Exception as exc:
        LOGGER.exception("[%s] Failed to set expirations: %s", CALLER_SCRIPT, exc)
        return {k: False for k in keys}


def update_redbeat_schedule_key(
    schedule_name: str,
    cron_expression: str,
    enabled: bool,
    redbeat_key_prefix: str,
    redis_env: RedisOrSentinel,
) -> bool:
    """Update the redbeat::schedule zset with next-run or remove when disabled.

    - Adds name->score where score is next UTC epoch seconds (float) minus 1,
      with a guard to avoid immediate-past enqueue by adding 60 seconds if too
      close to now.
    """
    try:
        if not schedule_name.startswith(redbeat_key_prefix):
            schedule_name = f"{redbeat_key_prefix}:{schedule_name}"
        if enabled:
            next_run = calculate_next_run_time(cron_expression) - 1
            if next_run <= int(datetime.now(timezone.utc).timestamp()) + 5:
                next_run += 60
            next_run_score = float(next_run)
            ok = update_redis_key(f"{redbeat_key_prefix}::schedule", {schedule_name: next_run_score}, redis_env)
            if not ok:
                LOGGER.error(
                    "Failed to add schedule %s to %s::schedule",
                    schedule_name,
                    redbeat_key_prefix,
                )
                return False
            LOGGER.info("Added schedule %s to %s::schedule", schedule_name, redbeat_key_prefix)
            return True
        # disabled: remove if present
        existing = get_redis_key_value(f"{redbeat_key_prefix}::schedule", redis_env)
        if not existing:
            LOGGER.info("No schedules found in %s::schedule", redbeat_key_prefix)
            return True
        if [s for s in existing if s[0] == schedule_name]:
            ok = remove_item_from_redis_key(f"{redbeat_key_prefix}::schedule", schedule_name, redis_env)
            if not ok:
                LOGGER.error("Failed removing %s from %s::schedule", schedule_name, redbeat_key_prefix)
                return False
            LOGGER.info("Removed schedule %s from %s::schedule", schedule_name, redbeat_key_prefix)
        return True
    except Exception as exc:
        LOGGER.exception("Error updating %s::schedule key: %s", redbeat_key_prefix, exc)
        return False


def bulk_update_redbeat_schedule_key(
    schedules: dict[str, dict[str, Any]],
    redbeat_key_prefix: str,
    redis_env: RedisOrSentinel,
) -> dict[str, bool]:
    """Bulk update redbeat::schedule for many schedules.

    Expected schedules mapping value:
    { name: {"cron_expression": str, "enabled": bool} }
    """
    success_dict: dict[str, bool] = {k: False for k in schedules}
    items_to_add: dict[str, float] = {}
    items_to_remove: list[str] = []
    try:
        for schedule_name, schedule_data in schedules.items():
            full_name = schedule_name if schedule_name.startswith(redbeat_key_prefix) else f"{redbeat_key_prefix}:{schedule_name}"
            if schedule_data.get("enabled", True):
                next_run = calculate_next_run_time(schedule_data["cron_expression"]) - 1
                if next_run <= int(datetime.now(timezone.utc).timestamp()) + 5:
                    next_run += 60
                items_to_add[full_name] = float(next_run)
                success_dict[schedule_name] = True  # assume add succeeds; corrected after op
            else:
                items_to_remove.append(full_name)
        if items_to_add:
            ok = update_redis_key(f"{redbeat_key_prefix}::schedule", items_to_add, redis_env)
            if not ok:
                LOGGER.error("Failed to add schedules to %s::schedule", redbeat_key_prefix)
                for k in items_to_add.keys():
                    success_dict[k] = False
            else:
                LOGGER.info("Added %d schedules to %s::schedule", len(items_to_add), redbeat_key_prefix)
        if items_to_remove:
            existing = get_redis_key_value(f"{redbeat_key_prefix}::schedule", redis_env) or []
            existing_names = {name for name, _ in existing}
            to_remove = [n for n in items_to_remove if n in existing_names]
            remove_results = bulk_remove_items_from_redis_key(f"{redbeat_key_prefix}::schedule", to_remove, redis_env)
            for key, ok in remove_results.items():
                if ok:
                    success_dict[key] = True
        return success_dict
    except Exception as exc:
        LOGGER.exception("Error bulk updating %s::schedule: %s", redbeat_key_prefix, exc)
        return success_dict
