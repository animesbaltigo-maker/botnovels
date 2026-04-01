import sqlite3
import time

from config import DATA_DIR

DB_PATH = DATA_DIR / "novel_referrals.sqlite"

MIN_INTERACTIONS_TO_QUALIFY = 2
MIN_SECONDS_TO_QUALIFY = 7 * 24 * 60 * 60


def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_referral_db():
    with _connect() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                created_at INTEGER,
                last_seen_at INTEGER,
                interactions INTEGER DEFAULT 0,
                is_blocked INTEGER DEFAULT 0
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals(
                referred_user_id INTEGER PRIMARY KEY,
                referrer_user_id INTEGER,
                created_at INTEGER,
                status TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS referral_clicks(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_user_id INTEGER,
                clicked_user_id INTEGER,
                created_at INTEGER
            )
            """
        )

        conn.commit()


def upsert_user(user_id, username=None, first_name=None):
    now = int(time.time())

    with _connect() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            INSERT OR IGNORE INTO users(
                user_id,
                username,
                first_name,
                created_at,
                last_seen_at,
                interactions,
                is_blocked
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                user_id,
                username or "",
                first_name or "",
                now,
                now,
                0,
                0,
            ),
        )

        cur.execute(
            """
            UPDATE users
            SET username = ?,
                first_name = ?,
                last_seen_at = ?
            WHERE user_id = ?
            """,
            (
                username or "",
                first_name or "",
                now,
                user_id,
            ),
        )

        conn.commit()


def register_interaction(user_id):
    now = int(time.time())

    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET interactions = interactions + 1,
                last_seen_at = ?
            WHERE user_id = ?
            """,
            (now, user_id),
        )
        conn.commit()


def mark_user_blocked(user_id, blocked=True):
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET is_blocked = ?
            WHERE user_id = ?
            """,
            (1 if blocked else 0, user_id),
        )
        conn.commit()


def register_referral_click(referrer, user):
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO referral_clicks(referrer_user_id, clicked_user_id, created_at)
            VALUES(?,?,?)
            """,
            (referrer, user, int(time.time())),
        )
        conn.commit()


def create_referral(referrer, user):
    if referrer == user:
        return False, "self"

    with _connect() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT referred_user_id, referrer_user_id
            FROM referrals
            WHERE referred_user_id = ?
            """,
            (user,),
        )

        row = cur.fetchone()
        if row:
            if int(row["referrer_user_id"]) == int(referrer):
                return False, "already_same"
            return False, "exists"

        cur.execute(
            """
            INSERT INTO referrals(referred_user_id, referrer_user_id, created_at, status)
            VALUES(?,?,?,?)
            """,
            (user, referrer, int(time.time()), "pending"),
        )
        conn.commit()

    return True, "ok"


def try_qualify_referral(user_id, is_channel_member):
    if not is_channel_member:
        return False, "pending_no_channel"

    with _connect() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT created_at, status
            FROM referrals
            WHERE referred_user_id = ?
            """,
            (user_id,),
        )
        referral = cur.fetchone()

        if not referral:
            return False, "no_referral"

        if referral["status"] != "pending":
            return False, referral["status"]

        cur.execute(
            """
            SELECT interactions, is_blocked
            FROM users
            WHERE user_id = ?
            """,
            (user_id,),
        )
        user = cur.fetchone()
        if not user:
            return False, "no_user"

        if int(user["is_blocked"]) == 1:
            return False, "blocked"

        age = int(time.time()) - int(referral["created_at"])
        interactions = int(user["interactions"])

        if age < MIN_SECONDS_TO_QUALIFY:
            return False, "pending_time"

        if interactions < MIN_INTERACTIONS_TO_QUALIFY:
            return False, "pending_interactions"

        cur.execute(
            """
            UPDATE referrals
            SET status = 'qualified'
            WHERE referred_user_id = ?
            """,
            (user_id,),
        )
        conn.commit()
        return True, "qualified"


def referral_stats(user_id):
    with _connect() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM referral_clicks
            WHERE referrer_user_id = ?
            """,
            (user_id,),
        )
        clicks = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM referrals
            WHERE referrer_user_id = ?
            """,
            (user_id,),
        )
        total = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM referrals
            WHERE referrer_user_id = ?
              AND status = 'pending'
            """,
            (user_id,),
        )
        pending = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM referrals
            WHERE referrer_user_id = ?
              AND status = 'qualified'
            """,
            (user_id,),
        )
        qualified = cur.fetchone()[0]

        return {
            "clicks": clicks,
            "total": total,
            "pending": pending,
            "qualified": qualified,
        }


def referral_ranking(limit=3):
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                u.user_id,
                u.username,
                u.first_name,
                COUNT(r.referred_user_id) as total
            FROM referrals r
            JOIN users u
              ON u.user_id = r.referrer_user_id
            WHERE r.status = 'qualified'
            GROUP BY r.referrer_user_id
            ORDER BY total DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()


def referral_admin_overview():
    with _connect() as conn:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM referral_clicks")
        clicks_total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM referrals")
        registered_total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM referrals WHERE status = 'pending'")
        pending_total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM referrals WHERE status = 'qualified'")
        approved_total = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM referrals
            WHERE status NOT IN ('pending', 'qualified')
            """
        )
        rejected_total = cur.fetchone()[0]

        return {
            "clicks_total": clicks_total,
            "registered_total": registered_total,
            "pending_total": pending_total,
            "approved_total": approved_total,
            "rejected_total": rejected_total,
        }


def get_all_pending_referrals():
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                r.referred_user_id,
                r.referrer_user_id,
                r.created_at,
                r.status,
                u.username,
                u.first_name,
                u.interactions,
                u.is_blocked
            FROM referrals r
            LEFT JOIN users u
              ON u.user_id = r.referred_user_id
            WHERE r.status = 'pending'
            ORDER BY r.created_at ASC
            """
        )
        return cur.fetchall()
