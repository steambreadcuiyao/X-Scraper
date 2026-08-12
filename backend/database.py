"""
SQLite database layer for X Scraper.
Schema + all CRUD operations with FTS5 full-text search.
"""

import sqlite3
import json
import os
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("database")

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
DB_PATH = os.path.join(DB_DIR, "x_scraper.db")


def get_conn() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT NOT NULL,
            cookies_json TEXT,
            proxy_config TEXT,
            status TEXT DEFAULT 'active',
            last_validated_at TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS account_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            color TEXT DEFAULT '#378ADD',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            display_name TEXT,
            avatar_url TEXT,
            platform_user_id TEXT,
            group_id INTEGER,
            tags TEXT DEFAULT '[]',
            status TEXT DEFAULT 'active',
            notes TEXT,
            last_scraped_at TEXT,
            tweet_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (group_id) REFERENCES account_groups(id)
        );

        CREATE TABLE IF NOT EXISTS scrape_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            scope_type TEXT DEFAULT 'account',
            scope_ids TEXT DEFAULT '[]',
            channel TEXT DEFAULT 'playwright',
            schedule_type TEXT DEFAULT 'manual',
            cron_expr TEXT,
            interval_minutes INTEGER,
            max_tweets_per_run INTEGER DEFAULT 5,
            status TEXT DEFAULT 'pending',
            last_run_at TEXT,
            next_run_at TEXT,
            progress_current INTEGER DEFAULT 0,
            progress_total INTEGER DEFAULT 0,
            error_message TEXT,
            run_count INTEGER DEFAULT 0,
            filters_json TEXT DEFAULT '{}',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS task_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            started_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            finished_at TEXT,
            status TEXT DEFAULT 'running',
            new_tweets INTEGER DEFAULT 0,
            accounts_total INTEGER DEFAULT 0,
            accounts_success INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            error_message TEXT,
            FOREIGN KEY (task_id) REFERENCES scrape_tasks(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS app_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            logger TEXT,
            message TEXT,
            task_id INTEGER,
            run_id INTEGER,
            error_type TEXT,
            traceback TEXT,
            extra_json TEXT DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS tweets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id TEXT NOT NULL UNIQUE,
            account_id INTEGER,
            author_username TEXT NOT NULL,
            author_display_name TEXT,
            text TEXT,
            translated_text TEXT,
            media_urls TEXT DEFAULT '[]',
            quoted_tweet_id TEXT,
            likes INTEGER DEFAULT 0,
            retweets INTEGER DEFAULT 0,
            replies INTEGER DEFAULT 0,
            views INTEGER DEFAULT 0,
            bookmarks INTEGER DEFAULT 0,
            language TEXT,
            is_pinned INTEGER DEFAULT 0,
            is_reply INTEGER DEFAULT 0,
            is_article INTEGER DEFAULT 0,
            url TEXT,
            tweet_type TEXT DEFAULT 'unknown',
            retweet_author TEXT DEFAULT '',
            sentiment TEXT,
            created_at TEXT,
            scraped_at TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS tweets_fts USING fts5(
            text, author_username, author_display_name,
            content=tweets, content_rowid=id
        );

        -- Triggers to keep FTS in sync
        CREATE TRIGGER IF NOT EXISTS tweets_ai AFTER INSERT ON tweets BEGIN
            INSERT INTO tweets_fts(rowid, text, author_username, author_display_name)
            VALUES (new.id, new.text, new.author_username, new.author_display_name);
        END;

        CREATE TRIGGER IF NOT EXISTS tweets_ad AFTER DELETE ON tweets BEGIN
            INSERT INTO tweets_fts(tweets_fts, rowid, text, author_username, author_display_name)
            VALUES ('delete', old.id, old.text, old.author_username, old.author_display_name);
        END;

        CREATE TRIGGER IF NOT EXISTS tweets_au AFTER UPDATE ON tweets BEGIN
            INSERT INTO tweets_fts(tweets_fts, rowid, text, author_username, author_display_name)
            VALUES ('delete', old.id, old.text, old.author_username, old.author_display_name);
            INSERT INTO tweets_fts(rowid, text, author_username, author_display_name)
            VALUES (new.id, new.text, new.author_username, new.author_display_name);
        END;

        CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author_username);
        CREATE INDEX IF NOT EXISTS idx_tweets_created ON tweets(created_at);
        CREATE INDEX IF NOT EXISTS idx_tweets_scraped ON tweets(scraped_at);
        CREATE INDEX IF NOT EXISTS idx_tweets_account ON tweets(account_id);
        CREATE INDEX IF NOT EXISTS idx_accounts_group ON accounts(group_id);
        CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
        CREATE INDEX IF NOT EXISTS idx_logs_ts ON app_logs(ts);
        CREATE INDEX IF NOT EXISTS idx_logs_level ON app_logs(level);
        CREATE INDEX IF NOT EXISTS idx_logs_task ON app_logs(task_id);
    """)
    conn.commit()
    conn.close()


def _migrate():
    """Add columns that may have been added after initial DB creation."""
    conn = get_conn()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(scrape_tasks)")}
    if "filters_json" not in cols:
        conn.execute("ALTER TABLE scrape_tasks ADD COLUMN filters_json TEXT DEFAULT '{}'")
    if "run_count" not in cols:
        conn.execute("ALTER TABLE scrape_tasks ADD COLUMN run_count INTEGER DEFAULT 0")
    tweet_cols = {r[1] for r in conn.execute("PRAGMA table_info(tweets)")}
    if "tweet_type" not in tweet_cols:
        conn.execute("ALTER TABLE tweets ADD COLUMN tweet_type TEXT DEFAULT 'unknown'")
    if "retweet_author" not in tweet_cols:
        conn.execute("ALTER TABLE tweets ADD COLUMN retweet_author TEXT DEFAULT ''")
    conn.commit()
    conn.close()


# ==================== Credential CRUD ====================

def cred_list(status: Optional[str] = None):
    conn = get_conn()
    q = "SELECT * FROM credentials"
    if status:
        q += " WHERE status=?"
        rows = conn.execute(q, (status,)).fetchall()
    else:
        rows = conn.execute(q).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def cred_create(label: str, cookies_json: str = None, proxy_config: str = None):
    conn = get_conn()
    c = conn.execute(
        "INSERT INTO credentials (label, cookies_json, proxy_config) VALUES (?,?,?)",
        (label, cookies_json, proxy_config)
    )
    conn.commit()
    rid = c.lastrowid
    row = conn.execute("SELECT * FROM credentials WHERE id=?", (rid,)).fetchone()
    conn.close()
    return dict(row)


def cred_update(cred_id: int, **kwargs):
    allowed = {"label", "cookies_json", "proxy_config", "status", "last_validated_at"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return None
    conn = get_conn()
    sets = ", ".join(f"{k}=?" for k in updates)
    vals = list(updates.values()) + [cred_id]
    conn.execute(f"UPDATE credentials SET {sets} WHERE id=?", vals)
    conn.commit()
    row = conn.execute("SELECT * FROM credentials WHERE id=?", (cred_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def cred_delete(cred_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM credentials WHERE id=?", (cred_id,))
    conn.commit()
    conn.close()


# ==================== Account Group CRUD ====================

def group_list():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM account_groups ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def group_create(name: str, color: str = "#378ADD"):
    conn = get_conn()
    try:
        c = conn.execute("INSERT INTO account_groups (name, color) VALUES (?,?)", (name, color))
        conn.commit()
        rid = c.lastrowid
        row = conn.execute("SELECT * FROM account_groups WHERE id=?", (rid,)).fetchone()
        conn.close()
        return dict(row)
    except sqlite3.IntegrityError:
        conn.close()
        raise ValueError(f"分组 '{name}' 已存在")


def group_update(group_id: int, **kwargs):
    allowed = {"name", "color"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return None
    conn = get_conn()
    sets = ", ".join(f"{k}=?" for k in updates)
    vals = list(updates.values()) + [group_id]
    conn.execute(f"UPDATE account_groups SET {sets} WHERE id=?", vals)
    conn.commit()
    row = conn.execute("SELECT * FROM account_groups WHERE id=?", (group_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def group_delete(group_id: int):
    conn = get_conn()
    conn.execute("UPDATE accounts SET group_id=NULL WHERE group_id=?", (group_id,))
    conn.execute("DELETE FROM account_groups WHERE id=?", (group_id,))
    conn.commit()
    conn.close()


# ==================== Account CRUD ====================

def account_list(status: Optional[str] = None, group_id: Optional[int] = None, tag: Optional[str] = None, username: Optional[str] = None, display_name: Optional[str] = None):
    conn = get_conn()
    q = """
        SELECT a.*, g.name as group_name, g.color as group_color
        FROM accounts a
        LEFT JOIN account_groups g ON a.group_id = g.id
        WHERE 1=1
    """
    params = []
    if status:
        q += " AND a.status=?"
        params.append(status)
    if group_id is not None:
        q += " AND a.group_id=?"
        params.append(group_id)
    if tag:
        tags = [t.strip() for t in tag.split(",") if t.strip()]
        if tags:
            clauses = []
            for t in tags:
                # Match both plain Chinese and unicode-escaped forms
                escaped = json.dumps(t, ensure_ascii=True).strip('"')
                clauses.append("(a.tags LIKE ? OR a.tags LIKE ?)")
                params.append(f'%"{t}"%')
                params.append(f'%"{escaped}"%')
            q += " AND (" + " OR ".join(clauses) + ")"
    if username:
        q += " AND a.username LIKE ?"
        params.append(f"%{username}%")
    if display_name:
        q += " AND a.display_name LIKE ?"
        params.append(f"%{display_name}%")
    q += " ORDER BY a.username"
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def account_create(username: str, display_name: str = None, avatar_url: str = None,
                   group_id: int = None, tags: list = None, notes: str = None):
    conn = get_conn()
    try:
        c = conn.execute(
            "INSERT INTO accounts (username, display_name, avatar_url, group_id, tags, notes) VALUES (?,?,?,?,?,?)",
            (username, display_name, avatar_url, group_id, json.dumps(tags or [], ensure_ascii=False), notes)
        )
        conn.commit()
        rid = c.lastrowid
        row = conn.execute("""
            SELECT a.*, g.name as group_name, g.color as group_color
            FROM accounts a LEFT JOIN account_groups g ON a.group_id=g.id
            WHERE a.id=?
        """, (rid,)).fetchone()
        conn.close()
        return dict(row)
    except sqlite3.IntegrityError:
        conn.close()
        raise ValueError(f"账号 '{username}' 已存在")


def account_update(acc_id: int, **kwargs):
    allowed = {"display_name", "avatar_url", "platform_user_id", "group_id", "tags",
               "status", "notes", "last_scraped_at", "tweet_count"}
    updates = {}
    for k, v in kwargs.items():
        if k in allowed:
            updates[k] = json.dumps(v, ensure_ascii=False) if k == "tags" and isinstance(v, list) else v
    if not updates:
        return None
    conn = get_conn()
    sets = ", ".join(f"{k}=?" for k in updates)
    vals = list(updates.values()) + [acc_id]
    conn.execute(f"UPDATE accounts SET {sets} WHERE id=?", vals)
    conn.commit()
    row = conn.execute("""
        SELECT a.*, g.name as group_name, g.color as group_color
        FROM accounts a LEFT JOIN account_groups g ON a.group_id=g.id
        WHERE a.id=?
    """, (acc_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def account_delete(acc_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM accounts WHERE id=?", (acc_id,))
    conn.commit()
    conn.close()


def account_batch_import(usernames: list, group_id: int = None):
    """Batch import usernames, skip duplicates."""
    conn = get_conn()
    added = 0
    skipped = 0
    for u in usernames:
        u = u.strip().lstrip("@")
        if not u:
            continue
        try:
            conn.execute(
                "INSERT INTO accounts (username, group_id) VALUES (?,?)",
                (u, group_id)
            )
            added += 1
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    conn.close()
    return {"added": added, "skipped": skipped}


# ==================== Scrape Task CRUD ====================

def task_list(status: Optional[str] = None):
    conn = get_conn()
    q = "SELECT * FROM scrape_tasks"
    if status:
        q += " WHERE status=?"
        rows = conn.execute(q, (status,)).fetchall()
    else:
        rows = conn.execute(q + " ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def task_create(name: str, scope_type: str = "account", scope_ids: list = None,
                channel: str = "playwright", schedule_type: str = "manual",
                cron_expr: str = None, interval_minutes: int = None,
                max_tweets_per_run: int = 5, filters_json: str = "{}"):
    conn = get_conn()
    c = conn.execute(
        """INSERT INTO scrape_tasks
           (name, scope_type, scope_ids, channel, schedule_type, cron_expr, interval_minutes, max_tweets_per_run, filters_json)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (name, scope_type, json.dumps(scope_ids or []),
         channel, schedule_type, cron_expr, interval_minutes, max_tweets_per_run, filters_json)
    )
    conn.commit()
    rid = c.lastrowid
    row = conn.execute("SELECT * FROM scrape_tasks WHERE id=?", (rid,)).fetchone()
    conn.close()
    return dict(row)


def task_update(task_id: int, **kwargs):
    allowed = {"name", "scope_type", "scope_ids", "channel", "schedule_type",
               "cron_expr", "interval_minutes", "max_tweets_per_run", "status",
               "credential_id",
               "last_run_at", "next_run_at", "progress_current", "progress_total",
               "error_message", "filters_json", "run_count"}
    updates = {}
    for k, v in kwargs.items():
        if k in allowed:
            updates[k] = json.dumps(v) if k in ("scope_ids",) and isinstance(v, (list, dict)) else v
    if not updates:
        return None
    conn = get_conn()
    sets = ", ".join(f"{k}=?" for k in updates)
    vals = list(updates.values()) + [task_id]
    conn.execute(f"UPDATE scrape_tasks SET {sets} WHERE id=?", vals)
    conn.commit()
    row = conn.execute("SELECT * FROM scrape_tasks WHERE id=?", (task_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def task_run_insert(task_id: int, started_at: str = None) -> int:
    """Insert a task run record, returns run_id."""
    from datetime import datetime
    conn = get_conn()
    ts = started_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c = conn.execute("INSERT INTO task_runs(task_id, started_at) VALUES(?,?)", (task_id, ts))
    conn.commit()
    run_id = c.lastrowid
    conn.close()
    return run_id


def task_run_finish(run_id: int, new_tweets: int = 0, error_message: str = None,
                    accounts_total: int = 0, accounts_success: int = 0, error_count: int = 0):
    """Mark a task run as finished."""
    from datetime import datetime
    status = "failed" if error_message else "success"
    conn = get_conn()
    conn.execute(
        """UPDATE task_runs SET finished_at=?, status=?, new_tweets=?,
           accounts_total=?, accounts_success=?, error_count=?, error_message=?
           WHERE id=?""",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), status, new_tweets,
         accounts_total, accounts_success, error_count, error_message, run_id)
    )
    conn.commit()
    conn.close()


def task_runs_list(task_id: int, limit: int = 5, offset: int = 0) -> dict:
    """Get paginated run history for a task. Returns {items, total, limit, offset}."""
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM task_runs WHERE task_id=?", (task_id,)).fetchone()[0]
    rows = conn.execute(
        "SELECT * FROM task_runs WHERE task_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
        (task_id, limit, offset)
    ).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows], "total": total, "limit": limit, "offset": offset}


def task_delete(task_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM scrape_tasks WHERE id=?", (task_id,))
    conn.commit()
    conn.close()


def task_resolve_scope(scope_type: str, scope_ids: list) -> list:
    """Resolve scope (account_ids / group_ids / tags) to actual account list."""
    if scope_type == "account":
        if not scope_ids:
            return []
        conn = get_conn()
        placeholders = ",".join("?" * len(scope_ids))
        rows = conn.execute(
            f"SELECT * FROM accounts WHERE id IN ({placeholders}) AND status='active'",
            scope_ids
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    elif scope_type == "group":
        if not scope_ids:
            return []
        conn = get_conn()
        placeholders = ",".join("?" * len(scope_ids))
        rows = conn.execute(
            f"SELECT * FROM accounts WHERE group_id IN ({placeholders}) AND status='active'",
            scope_ids
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    elif scope_type == "tag":
        if not scope_ids:
            return []
        conn = get_conn()
        accounts = []
        for tag in scope_ids:
            rows = conn.execute(
                "SELECT * FROM accounts WHERE tags LIKE ? AND status='active'",
                (f'%"{tag}"%',)
            ).fetchall()
            for r in rows:
                d = dict(r)
                if d not in accounts:
                    accounts.append(d)
        conn.close()
        return accounts
    return []


# ==================== Tweet CRUD & Search ====================

def get_last_tweet_id(account_id: int) -> Optional[str]:
    """Get the latest tweet_id for an account (for incremental scraping)."""
    conn = get_conn()
    row = conn.execute(
        "SELECT tweet_id FROM tweets WHERE account_id=? ORDER BY tweet_id DESC LIMIT 1",
        (account_id,)
    ).fetchone()
    conn.close()
    return row["tweet_id"] if row else None


def get_existing_tweet_ids(account_id: int, limit: int = 200) -> list[str]:
    """Get recent existing tweet_ids for this account, used for quick duplicate check."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT tweet_id FROM tweets WHERE account_id=? ORDER BY tweet_id DESC LIMIT ?",
        (account_id, limit)
    ).fetchall()
    conn.close()
    return [r["tweet_id"] for r in rows]


def tweet_insert(tweet_data: dict) -> Optional[dict]:
    """Insert or ignore duplicate tweet by tweet_id."""
    conn = get_conn()
    try:
        c = conn.execute("""
            INSERT OR IGNORE INTO tweets
            (tweet_id, account_id, author_username, author_display_name, text,
             media_urls, quoted_tweet_id, likes, retweets, replies, views,
             bookmarks, language, is_pinned, is_reply, is_article, url, tweet_type, retweet_author, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            tweet_data["tweet_id"],
            tweet_data.get("account_id"),
            tweet_data["author_username"],
            tweet_data.get("author_display_name"),
            tweet_data.get("text"),
            json.dumps(tweet_data.get("media_urls", [])),
            tweet_data.get("quoted_tweet_id"),
            tweet_data.get("likes", 0),
            tweet_data.get("retweets", 0),
            tweet_data.get("replies", 0),
            tweet_data.get("views", 0),
            tweet_data.get("bookmarks", 0),
            tweet_data.get("language"),
            tweet_data.get("is_pinned", 0),
            tweet_data.get("is_reply", 0),
            tweet_data.get("is_article", 0),
            tweet_data.get("url"),
            tweet_data.get("tweet_type", "unknown"),
            tweet_data.get("retweet_author", ""),
            tweet_data.get("created_at"),
        ))
        conn.commit()
        if c.lastrowid > 0:
            row = conn.execute("SELECT * FROM tweets WHERE id=?", (c.lastrowid,)).fetchone()
            conn.close()
            return dict(row)
        conn.close()
        return None
    except Exception as e:
        conn.close()
        raise e


def tweet_search(keyword: str = None, author: str = None,
                 group_id: int = None, tag: str = None,
                 date_from: str = None, date_to: str = None,
                 sentiment: str = None, tweet_type: str = None,
                 limit: int = 100, offset: int = 0) -> dict:
    """Search tweets with filters. Uses FTS5 for keyword search."""
    conn = get_conn()

    if keyword:
        base_q = """
            SELECT t.*, a.group_id, g.name as group_name
            FROM tweets_fts fts
            JOIN tweets t ON fts.rowid = t.id
            LEFT JOIN accounts a ON t.account_id = a.id
            LEFT JOIN account_groups g ON a.group_id = g.id
            WHERE tweets_fts MATCH ?
        """
        params = [keyword]
    else:
        base_q = """
            SELECT t.*, a.group_id, g.name as group_name
            FROM tweets t
            LEFT JOIN accounts a ON t.account_id = a.id
            LEFT JOIN account_groups g ON a.group_id = g.id
            WHERE 1=1
        """
        params = []

    if author:
        base_q += " AND t.author_username=?"
        params.append(author)
    if group_id is not None:
        base_q += " AND a.group_id=?"
        params.append(group_id)
    if tag:
        base_q += " AND a.tags LIKE ?"
        params.append(f'%"{tag}"%')
    if date_from:
        base_q += " AND t.created_at >= ?"
        params.append(date_from)
    if date_to:
        base_q += " AND t.created_at <= ?"
        params.append(date_to)
    if sentiment:
        base_q += " AND t.sentiment=?"
        params.append(sentiment)
    if tweet_type:
        base_q += " AND t.tweet_type=?"
        params.append(tweet_type)

    # Count
    count_q = base_q.replace(
        "SELECT t.*, a.group_id, g.name as group_name",
        "SELECT COUNT(*) as cnt"
    )
    total = conn.execute(count_q, params).fetchone()["cnt"]

    # Paginated results
    data_q = base_q + " ORDER BY t.created_at DESC LIMIT ? OFFSET ?"
    rows = conn.execute(data_q, params + [limit, offset]).fetchall()
    conn.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [dict(r) for r in rows]
    }


def task_covered_account_ids(conn):
    """Return set of account IDs covered by all active tasks (all scope types)."""
    tasks = conn.execute(
        "SELECT scope_type, scope_ids FROM scrape_tasks WHERE status='active'"
    ).fetchall()
    covered = set()
    for t in tasks:
        scope_type = t["scope_type"]
        try:
            scope_ids = json.loads(t["scope_ids"])
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(scope_ids, dict):
            raw_ids = scope_ids.get("accounts", [])
        else:
            raw_ids = scope_ids
        if not raw_ids:
            continue
        if scope_type == "account":
            ids = conn.execute(
                f"SELECT id FROM accounts WHERE id IN ({','.join('?'*len(raw_ids))}) AND status='active'",
                raw_ids
            ).fetchall()
            covered.update(r["id"] for r in ids)
        elif scope_type == "group":
            ids = conn.execute(
                f"SELECT id FROM accounts WHERE group_id IN ({','.join('?'*len(raw_ids))}) AND status='active'",
                raw_ids
            ).fetchall()
            covered.update(r["id"] for r in ids)
        elif scope_type == "tag":
            for tag in raw_ids:
                ids = conn.execute(
                    "SELECT id FROM accounts WHERE tags LIKE ? AND status='active'",
                    (f'%"{tag}"%',)
                ).fetchall()
                covered.update(r["id"] for r in ids)
    return covered


def task_covered_account_count(conn=None):
    """Count unique accounts covered by all active tasks (all scope types)."""
    own_conn = conn is None
    if own_conn:
        conn = get_conn()
    count = len(task_covered_account_ids(conn))
    if own_conn:
        conn.close()
    return count


def tweet_stats():
    """Get overview statistics."""
    conn = get_conn()
    stats = {}
    stats["total_tweets"] = conn.execute("SELECT COUNT(*) as c FROM tweets").fetchone()["c"]
    stats["total_accounts"] = conn.execute("SELECT COUNT(*) as c FROM accounts").fetchone()["c"]
    stats["active_accounts"] = conn.execute(
        "SELECT COUNT(*) as c FROM accounts WHERE status='active'"
    ).fetchone()["c"]
    stats["task_covered_accounts"] = task_covered_account_count(conn)
    stats["recent_count"] = conn.execute(
        "SELECT COUNT(*) as c FROM tweets WHERE scraped_at >= datetime('now','localtime','-24 hours')"
    ).fetchone()["c"]
    stats["latest_scrape"] = conn.execute(
        "SELECT MAX(scraped_at) as t FROM tweets"
    ).fetchone()["t"]

    # Top authors
    top = conn.execute("""
        SELECT author_username, COUNT(*) as cnt
        FROM tweets GROUP BY author_username ORDER BY cnt DESC LIMIT 5
    """).fetchall()
    stats["top_authors"] = [dict(r) for r in top]

    conn.close()
    return stats


def tweet_export_csv(keyword: str = None, author: str = None,
                     group_id: int = None, tag: str = None,
                     date_from: str = None, date_to: str = None,
                     sentiment: str = None, tweet_type: str = None,
                     tweet_ids: list = None) -> str:
    """Export tweets as CSV string."""
    import csv
    import io

    if tweet_ids:
        conn = get_conn()
        placeholders = ",".join("?" * len(tweet_ids))
        rows = conn.execute(
            f"""SELECT t.*, a.group_id, g.name as group_name
                FROM tweets t
                LEFT JOIN accounts a ON t.account_id = a.id
                LEFT JOIN account_groups g ON a.group_id = g.id
                WHERE t.id IN ({placeholders})
                ORDER BY t.created_at DESC""",
            tweet_ids
        ).fetchall()
        conn.close()
    else:
        result = tweet_search(keyword=keyword, author=author, group_id=group_id,
                              tag=tag, date_from=date_from, date_to=date_to,
                              sentiment=sentiment, tweet_type=tweet_type, limit=100000, offset=0)
        rows = result["items"]  # Already dicts, no need to re-query

    output = io.StringIO()
    if rows:
        fieldnames = list(rows[0].keys()) if hasattr(rows[0], 'keys') else list(dict(rows[0]).keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row) if not isinstance(row, dict) else row)
    return output.getvalue()


# ==================== Log Storage ====================

def log_batch_insert(entries: list):
    """Batch insert log entries. Each entry is a dict with ts, level, message, etc."""
    if not entries:
        return
    # Ensure every entry has all required keys (executemany with named params is strict)
    for e in entries:
        e.setdefault("task", None)
        e.setdefault("run", None)
        e.setdefault("error_type", None)
        e.setdefault("traceback", None)
    conn = get_conn()
    conn.executemany(
        """INSERT INTO app_logs (ts, level, logger, message, task_id, run_id, error_type, traceback)
           VALUES (:ts, :level, :logger, :message, :task, :run, :error_type, :traceback)""",
        entries
    )
    conn.commit()
    # Rotate: keep last 10000 entries
    conn.execute("DELETE FROM app_logs WHERE id < (SELECT MAX(id)-10000 FROM app_logs)")
    conn.commit()
    conn.close()


def log_search(level: Optional[str] = None, keyword: Optional[str] = None,
               task_id: Optional[int] = None, limit: int = 200, offset: int = 0) -> dict:
    """Search logs with filters, returns paginated results."""
    conn = get_conn()
    q = "SELECT * FROM app_logs WHERE 1=1"
    cq = "SELECT COUNT(*) FROM app_logs WHERE 1=1"
    params = []
    if level and level.upper() != "ALL":
        q += " AND level=?"
        cq += " AND level=?"
        params.append(level.upper())
    if keyword:
        q += " AND message LIKE ?"
        cq += " AND message LIKE ?"
        params.append(f"%{keyword}%")
    if task_id is not None:
        q += " AND task_id=?"
        cq += " AND task_id=?"
        params.append(task_id)
    total = conn.execute(cq, params).fetchone()[0]
    q += " ORDER BY id DESC LIMIT ? OFFSET ?"
    rows = conn.execute(q, params + [limit, offset]).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows], "total": total, "limit": limit, "offset": offset}


# ==================== Init on import ====================
init_db()
_migrate()
