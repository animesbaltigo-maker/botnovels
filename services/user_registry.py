import json
from threading import Lock

from config import DATA_DIR

USERS_JSON_PATH = DATA_DIR / "novel_users.json"

_lock = Lock()
_users_cache: set[int] | None = None


def _load_users() -> list[int]:
    global _users_cache
    if _users_cache is not None:
        return sorted(_users_cache)

    if not USERS_JSON_PATH.exists():
        _users_cache = set()
        return []

    try:
        with USERS_JSON_PATH.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        return []

    if not isinstance(data, list):
        _users_cache = set()
        return []

    result: list[int] = []
    for item in data:
        try:
            result.append(int(item))
        except Exception:
            continue
    _users_cache = set(result)
    return result


def _save_users(users: list[int]) -> None:
    global _users_cache
    USERS_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    normalized = sorted(set(int(value) for value in users))
    with USERS_JSON_PATH.open("w", encoding="utf-8") as file:
        json.dump(normalized, file, ensure_ascii=False, indent=2)
    _users_cache = set(normalized)


def register_user(user_id: int | None) -> None:
    if not user_id:
        return

    with _lock:
        users = set(_load_users())
        value = int(user_id)
        if value not in users:
            users.add(value)
            _save_users(list(users))


def remove_user(user_id: int | None) -> None:
    if not user_id:
        return

    with _lock:
        users = {value for value in _load_users() if int(value) != int(user_id)}
        _save_users(list(users))


def get_all_users() -> list[int]:
    with _lock:
        return _load_users()


def get_total_users() -> int:
    return len(get_all_users())
