"""
X Scraper - FastAPI Backend (Playwright)
REST API + WebSocket + Background task scheduler.
"""

import json
import logging
import asyncio
import random
import sys

# On Windows, Playwright requires ProactorEventLoop for subprocess support.
# Must be set BEFORE any asyncio operations.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import os

from database import (
    cred_list, cred_create, cred_update, cred_delete,
    group_list, group_create, group_update, group_delete,
    account_list, account_create, account_update, account_delete,
    account_batch_import,
    task_list, task_create, task_update, task_delete, task_resolve_scope,
    tweet_search, tweet_stats, tweet_export_csv, get_last_tweet_id,
    get_existing_tweet_ids, tweet_insert, account_update,
    task_run_insert, task_run_finish, task_runs_list,
    log_search,
)
from scraper.playwright_scraper import validate_login, launch_x_login, BROWSER_MODE

from logging_config import setup_logging, get_logger, LogContext, DbLogHandler

setup_logging("INFO")
logger = get_logger("scraper")

scheduler = AsyncIOScheduler()
active_ws_clients: list = []
_shared_pw = None  # Playwright driver singleton, never stopped
_pw_lock = asyncio.Lock()  # Prevent concurrent driver init


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Playwright driver BEFORE scheduler to avoid race conditions
    global _shared_pw
    try:
        from playwright.async_api import async_playwright
        _shared_pw = await async_playwright().start()
        logger.info("Playwright driver 已初始化（启动时）")
    except Exception as e:
        logger.warning(f"Playwright driver 初始化失败: {e}")

    scheduler.start()
    logger.info("Scheduler started")

    # Schedule all active tasks and catch up missed interval runs
    from datetime import datetime, timedelta
    now = datetime.now()
    for t in task_list(status="active"):
        _schedule_task(t)
        if t["schedule_type"] == "interval" and t.get("interval_minutes"):
            try:
                last_dt = datetime.strptime(t["last_run_at"] or "2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                next_dt = last_dt + timedelta(minutes=t["interval_minutes"])
                if now > next_dt and (now - next_dt).total_seconds() < 7200:
                    logger.info(f"任务 #{t['id']} {t['name']} 错过调度 {next_dt.strftime('%H:%M')}，立即补跑")
                    asyncio.create_task(_execute_task(t["id"]))
            except Exception as e:
                logger.warning(f"补跑检查异常 #{t['id']}: {e}")
    yield
    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(title="X Scraper", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "frontend")

@app.get("/", response_class=HTMLResponse)
async def root():
    content = ""
    with open(os.path.join(FRONTEND_DIR, "index.html"), "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(
        content=content,
        headers={
            "X-Frame-Options": "SAMEORIGIN",
            "Content-Security-Policy": "default-src * 'unsafe-inline' 'unsafe-eval' data: blob:; frame-ancestors 'self'",
        }
    )


# ==================== WebSocket ====================

@app.websocket("/ws")
async def websocket_endpoint(ws):
    await ws.accept()
    active_ws_clients.append(ws)
    try:
        while True:
            await ws.receive_text()
    except Exception:
        if ws in active_ws_clients:
            active_ws_clients.remove(ws)


async def broadcast(msg: dict):
    dead = []
    for ws in active_ws_clients:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in active_ws_clients:
            active_ws_clients.remove(ws)


# ==================== Pydantic Models ====================

class CredCreate(BaseModel):
    label: str
    cookies_json: Optional[str] = None
    proxy_config: Optional[str] = None


class CredUpdate(BaseModel):
    label: Optional[str] = None
    cookies_json: Optional[str] = None
    proxy_config: Optional[str] = None
    status: Optional[str] = None


class GroupCreate(BaseModel):
    name: str
    color: str = "#378ADD"


class GroupUpdate(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None


class AccountCreate(BaseModel):
    username: str
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    group_id: Optional[int] = None
    tags: list[str] = []
    notes: Optional[str] = None


class AccountUpdate(BaseModel):
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    platform_user_id: Optional[str] = None
    group_id: Optional[int] = None
    tags: Optional[list[str]] = None
    status: Optional[str] = None
    notes: Optional[str] = None


class BatchImport(BaseModel):
    usernames: list[str]
    group_id: Optional[int] = None


class TaskCreate(BaseModel):
    name: str
    scope_type: str = "account"
    scope_ids: list = []
    schedule_type: str = "manual"
    cron_expr: Optional[str] = None
    interval_minutes: Optional[int] = None
    max_tweets_per_run: int = 5
    credential_id: Optional[int] = None
    filters_json: Optional[str] = None


class TaskUpdate(BaseModel):
    name: Optional[str] = None
    scope_type: Optional[str] = None
    scope_ids: Optional[list] = None
    schedule_type: Optional[str] = None
    cron_expr: Optional[str] = None
    interval_minutes: Optional[int] = None
    max_tweets_per_run: Optional[int] = None
    status: Optional[str] = None
    credential_id: Optional[int] = None
    filters_json: Optional[str] = None


# ==================== Credential Routes ====================

@app.get("/api/credentials")
def api_cred_list():
    return cred_list()


@app.post("/api/credentials")
def api_cred_create(data: CredCreate):
    return cred_create(data.label, data.cookies_json, data.proxy_config)


@app.put("/api/credentials/{cred_id}")
def api_cred_update(cred_id: int, data: CredUpdate):
    updates = {k: v for k, v in data.model_dump().items() if v is not None}
    result = cred_update(cred_id, **updates)
    if not result:
        raise HTTPException(404, "凭证不存在")
    return result


@app.delete("/api/credentials/{cred_id}")
def api_cred_delete(cred_id: int):
    cred_delete(cred_id)
    return {"ok": True}


@app.post("/api/credentials/{cred_id}/validate")
async def api_cred_validate(cred_id: int):
    """Validate login credentials by checking X home page."""
    creds = cred_list()
    cred = next((c for c in creds if c["id"] == cred_id), None)
    if not cred:
        raise HTTPException(404, "凭证不存在")
    if not cred["cookies_json"]:
        return {"valid": False, "error": "没有配置 Cookie"}
    try:
        import json as _json
        _json.loads(cred["cookies_json"])
    except Exception:
        return {"valid": False, "error": "Cookie 格式错误（需要 JSON 数组格式）"}
    try:
        result = await validate_login(cred["cookies_json"], cred.get("proxy_config"))
    except Exception as e:
        return {"valid": False, "error": f"验证失败: {str(e)[:100]}"}
    if result.get("valid"):
        cred_update(cred_id, status="active", last_validated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        cred_update(cred_id, status="expired", last_validated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return result


@app.post("/api/credentials/login")
async def api_cred_login(background_tasks: BackgroundTasks):
    """Launch Chrome for manual X login, extract cookies, save as credential."""

    async def _do_login():
        result = await launch_x_login()
        if "error" in result:
            logger.error(f"Login failed: {result['error']}")
            return
        cred_create(result["label"], result["cookies_json"])

    background_tasks.add_task(_do_login)
    return {"ok": True, "message": "浏览器已打开，请在浏览器中完成 X 登录。登录成功后凭证会自动保存。"}


# ==================== Group Routes ====================

@app.get("/api/groups")
def api_group_list():
    return group_list()


@app.post("/api/groups")
def api_group_create(data: GroupCreate):
    try:
        return group_create(data.name, data.color)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.put("/api/groups/{group_id}")
def api_group_update(group_id: int, data: GroupUpdate):
    updates = {k: v for k, v in data.model_dump().items() if v is not None}
    result = group_update(group_id, **updates)
    if not result:
        raise HTTPException(404, "分组不存在")
    return result


@app.delete("/api/groups/{group_id}")
def api_group_delete(group_id: int):
    group_delete(group_id)
    return {"ok": True}


# ==================== Account Routes ====================

@app.get("/api/accounts")
def api_account_list(status: Optional[str] = None, group_id: Optional[int] = None, tag: Optional[str] = None, username: Optional[str] = None, display_name: Optional[str] = None):
    return account_list(status=status, group_id=group_id, tag=tag, username=username, display_name=display_name)


@app.post("/api/accounts")
async def api_account_create(data: AccountCreate):
    from scraper.playwright_scraper import check_account_exists, PLAYWRIGHT_AVAILABLE
    if PLAYWRIGHT_AVAILABLE:
        exists = await check_account_exists(data.username)
        if not exists:
            raise HTTPException(400, f"账号 @{data.username} 不存在或为私密账号")
    try:
        return account_create(
            data.username, data.display_name, data.avatar_url,
            data.group_id, data.tags, data.notes
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.put("/api/accounts/{acc_id}")
def api_account_update(acc_id: int, data: AccountUpdate):
    updates = {k: v for k, v in data.model_dump().items() if v is not None}
    result = account_update(acc_id, **updates)
    if not result:
        raise HTTPException(404, "账号不存在")
    return result


@app.delete("/api/accounts/{acc_id}")
def api_account_delete(acc_id: int):
    account_delete(acc_id)
    return {"ok": True}


@app.post("/api/accounts/batch-import")
def api_account_batch_import(data: BatchImport):
    return account_batch_import(data.usernames, data.group_id)


# ==================== Task Routes ====================

@app.get("/api/tasks")
def api_task_list():
    return task_list()


@app.post("/api/tasks")
def api_task_create(data: TaskCreate):
    initial_status = "active" if data.schedule_type != "manual" else "pending"
    task = task_create(
        data.name, data.scope_type, data.scope_ids,
        "playwright", data.schedule_type, data.cron_expr,
        data.interval_minutes, data.max_tweets_per_run,
        filters_json=data.filters_json or "{}"
    )
    # Attach credential to scope_ids metadata
    if data.credential_id:
        scope_ids = json.loads(task["scope_ids"]) if isinstance(task["scope_ids"], str) else task["scope_ids"]
        task_update(task["id"], scope_ids={"credential_id": data.credential_id, "accounts": scope_ids})
    if initial_status == "active":
        task = task_update(task["id"], status="active")
    _schedule_task(task)
    return task


@app.put("/api/tasks/{task_id}")
def api_task_update(task_id: int, data: TaskUpdate):
    updates = {k: v for k, v in data.model_dump().items() if v is not None}
    result = task_update(task_id, **updates)
    if not result:
        raise HTTPException(404, "任务不存在")
    _remove_job(task_id)
    _schedule_task(result)
    return result


@app.delete("/api/tasks/{task_id}")
def api_task_delete(task_id: int):
    _remove_job(task_id)
    task_delete(task_id)
    return {"ok": True}


@app.get("/api/tasks/{task_id}/runs")
def api_task_runs(task_id: int, limit: int = 5, offset: int = 0):
    return task_runs_list(task_id, limit, offset)


@app.get("/api/logs")
def api_log_search(level: Optional[str] = None, keyword: Optional[str] = None,
                   task_id: Optional[int] = None, limit: int = 200, offset: int = 0):
    return log_search(level=level, keyword=keyword, task_id=task_id, limit=limit, offset=offset)


@app.post("/api/tasks/{task_id}/run")
async def api_task_run_now(task_id: int, background_tasks: BackgroundTasks):
    tasks = task_list()
    task = next((t for t in tasks if t["id"] == task_id), None)
    if not task:
        raise HTTPException(404, "任务不存在")
    asyncio.create_task(_execute_task(task_id))
    return {"ok": True, "message": "任务已触发，请在前端查看进度"}


# ==================== Tweet Routes ====================

@app.get("/api/tweets")
def api_tweet_search(
    keyword: Optional[str] = None,
    author: Optional[str] = None,
    group_id: Optional[int] = None,
    tag: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sentiment: Optional[str] = None,
    tweet_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    return tweet_search(
        keyword=keyword, author=author, group_id=group_id, tag=tag,
        date_from=date_from, date_to=date_to, sentiment=sentiment,
        tweet_type=tweet_type,
        limit=limit, offset=offset,
    )


@app.get("/api/tweets/export")
def api_tweet_export(
    keyword: Optional[str] = None,
    author: Optional[str] = None,
    group_id: Optional[int] = None,
    tag: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sentiment: Optional[str] = None,
    ids: Optional[str] = None,
):
    tweet_ids = None
    if ids:
        tweet_ids = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    csv_data = tweet_export_csv(
        keyword=keyword, author=author, group_id=group_id, tag=tag,
        date_from=date_from, date_to=date_to, sentiment=sentiment,
        tweet_ids=tweet_ids,
    )
    return PlainTextResponse(
        csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=tweets_export.csv"},
    )


# ==================== Stats ====================

@app.get("/api/stats")
def api_stats():
    return tweet_stats()


@app.get("/api/stats/trend")
def api_stats_trend():
    from database import get_conn
    conn = get_conn()
    rows = conn.execute("""
        SELECT date(created_at) as d, COUNT(*) as cnt
        FROM tweets WHERE created_at >= date('now','-6 days')
        GROUP BY d ORDER BY d
    """).fetchall()
    conn.close()


@app.get("/api/stats/uncovered-accounts")
def api_uncovered_accounts(page: int = 1, page_size: int = 10):
    """List accounts NOT covered by any active collection task."""
    from database import get_conn, task_covered_account_ids
    import json
    conn = get_conn()
    covered_ids = task_covered_account_ids(conn)
    if covered_ids:
        placeholders = ",".join("?" * len(covered_ids))
        total = conn.execute(
            f"SELECT COUNT(*) FROM accounts WHERE status='active' AND id NOT IN ({placeholders})",
            list(covered_ids)
        ).fetchone()[0]
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""SELECT a.*, g.name as group_name
                FROM accounts a
                LEFT JOIN account_groups g ON a.group_id = g.id
                WHERE a.status='active' AND a.id NOT IN ({placeholders})
                ORDER BY a.created_at DESC
                LIMIT ? OFFSET ?""",
            list(covered_ids) + [page_size, offset]
        ).fetchall()
    else:
        total = conn.execute(
            "SELECT COUNT(*) FROM accounts WHERE status='active'"
        ).fetchone()[0]
        offset = (page - 1) * page_size
        rows = conn.execute(
            """SELECT a.*, g.name as group_name
                FROM accounts a
                LEFT JOIN account_groups g ON a.group_id = g.id
                WHERE a.status='active'
                ORDER BY a.created_at DESC
                LIMIT ? OFFSET ?""",
            (page_size, offset)
        ).fetchall()
    conn.close()
    items = []
    for r in rows:
        tags = r["tags"]
        try:
            tags = json.loads(tags) if tags else []
        except (json.JSONDecodeError, TypeError):
            tags = []
        items.append({
            "id": r["id"],
            "username": r["username"],
            "display_name": r["display_name"],
            "group_name": r["group_name"] or "未分组",
            "tags": tags,
            "created_at": r["created_at"],
        })
    return {"total": total, "page": page, "page_size": page_size, "items": items}
    dates = [r['d'] for r in rows]
    counts = [r['cnt'] for r in rows]
    return {"dates": dates, "counts": counts}


# ==================== Scheduler ====================

def _remove_job(task_id: int):
    job_id = f"task_{task_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)


def _schedule_task(task: dict):
    _remove_job(task["id"])
    if task["status"] != "active":
        return
    job_id = f"task_{task['id']}"

    if task["schedule_type"] == "interval" and task.get("interval_minutes"):
        from datetime import datetime
        now = datetime.now()
        start_date = now  # base for interval: fires at start_date + N*interval
        if task.get("last_run_at"):
            try:
                last_dt = datetime.strptime(task["last_run_at"], "%Y-%m-%d %H:%M:%S")
                next_dt = last_dt + timedelta(minutes=task["interval_minutes"])
                if now < next_dt:
                    start_date = last_dt  # next fire = last_run_at + interval (future)
            except (ValueError, TypeError):
                pass
        scheduler.add_job(
            _execute_task, "interval", minutes=task["interval_minutes"],
            args=[task["id"]], id=job_id, replace_existing=True,
            max_instances=1, coalesce=True, misfire_grace_time=3600,
            start_date=start_date,
        )
        # Update next_run_at for display
        job = scheduler.get_job(job_id)
        if job and job.next_run_time:
            task_update(task["id"], next_run_at=job.next_run_time.strftime("%Y-%m-%d %H:%M:%S"))
    elif task["schedule_type"] == "cron" and task.get("cron_expr"):
        parts = task["cron_expr"].split()
        if len(parts) >= 5:
            scheduler.add_job(
                _execute_task, "cron",
                minute=parts[0], hour=parts[1], day=parts[2],
                month=parts[3], day_of_week=parts[4],
                args=[task["id"]], id=job_id, replace_existing=True,
                max_instances=1,
            )


def _resolve_credentials(task: dict) -> tuple:
    """Get cookies and proxy for a task from its credential_id."""
    scope_ids = task.get("scope_ids", "[]")
    if isinstance(scope_ids, str):
        try:
            scope_ids = json.loads(scope_ids)
        except Exception:
            scope_ids = []

    # Check if scope_ids contains credential info
    cred_id = None
    if isinstance(scope_ids, dict):
        cred_id = scope_ids.get("credential_id")

    if not cred_id:
        # Fall back to first active credential
        creds = cred_list(status="active")
        if creds:
            cred = creds[0]
            return cred.get("cookies_json"), cred.get("proxy_config")
        return None, None

    creds = cred_list()
    cred = next((c for c in creds if c["id"] == cred_id), None)
    if cred:
        return cred.get("cookies_json"), cred.get("proxy_config")
    return None, None


def _get_account_scope_ids(task: dict) -> list:
    """Extract account/group/tag IDs from task scope_ids."""
    scope_ids = task.get("scope_ids", "[]")
    if isinstance(scope_ids, str):
        try:
            scope_ids = json.loads(scope_ids)
        except Exception:
            return []

    if isinstance(scope_ids, dict):
        return scope_ids.get("accounts", [])
    return scope_ids


async def _execute_task(task_id: int):
    try:
        await _execute_task_inner(task_id)
    except Exception as e:
        import traceback
        logger.error(f"任务 #{task_id} 异常崩溃: {e}", exc_info=True)
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            tasks = task_list()
            task = next((t for t in tasks if t["id"] == task_id), None)
            new_status = "completed" if (task and task["schedule_type"] == "manual") else "active"
            task_update(task_id, status=new_status, last_run_at=now,
                        error_message=str(e)[:200])
        except Exception:
            pass

        # Also mark the current run as failed
        try:
            from database import get_conn
            conn = get_conn()
            conn.execute("UPDATE task_runs SET status='failed', finished_at=datetime('now','localtime'), error_message=? WHERE task_id=? AND status='running'", (f"{type(e).__name__}: {str(e)[:200]}", task_id))
            conn.commit()
            conn.close()
        except Exception:
            pass

        # Re-schedule to prevent scheduler freeze after crash
        try:
            tasks = task_list()
            task = next((t for t in tasks if t["id"] == task_id), None)
            if task and task["schedule_type"] != "manual":
                _schedule_task(task)
        except Exception:
            pass


async def _execute_task_inner(task_id: int):
    tasks = task_list()
    task = next((t for t in tasks if t["id"] == task_id), None)
    if not task:
        return

    task_update(task_id, status="running", progress_current=0, error_message=None)

    run_id = task_run_insert(task_id)
    LogContext.set(task_id=task_id, run_id=run_id)

    _filters = {}
    try:
        fj = task.get("filters_json", "{}")
        _filters = json.loads(fj) if isinstance(fj, str) else fj
    except Exception:
        pass

    cookies_json, proxy = _resolve_credentials(task)
    scope_ids = _get_account_scope_ids(task)
    accounts = task_resolve_scope(task["scope_type"], scope_ids)
    total = len(accounts)

    logger.info(f"任务开始执行: #{task_id} {task['name']}, {total}个账号")

    task_update(task_id, progress_total=total)
    all_errors = []
    new_total = 0
    accounts_success = 0

    ctx = None
    browser = None
    page = None
    pw = None
    try:
        from scraper.playwright_scraper import _make_browser, _make_context, fetch_timeline_with_page, BROWSER_MODE as _bm

        if _bm == "cdp":
            # CDP mode: fresh driver per run (shared singleton is unreliable for CDP connections)
            pw, browser = await _make_browser()
        else:
            pw = _shared_pw
            if not pw:
                raise RuntimeError("Playwright driver 未初始化")
            _, browser = await _make_browser(pw)
        ctx = await _make_context(browser, cookies_json, proxy)
        page = await ctx.new_page()

        for i, acc in enumerate(accounts):
            retry = 0
            while retry <= 2:
                try:
                    last_tweet_id = get_last_tweet_id(acc["id"])
                    existing_ids = set(get_existing_tweet_ids(acc["id"], limit=300))
                    tweets = await fetch_timeline_with_page(page, acc["username"], task["max_tweets_per_run"],
                                                      last_tweet_id=last_tweet_id, existing_ids=existing_ids, filters=_filters)
                    for t in tweets:
                        t["account_id"] = acc["id"]
                        if tweet_insert(t):
                            new_total += 1
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    account_update(acc["id"], last_scraped_at=now)
                    accounts_success += 1
                    break
                except Exception as e:
                    err_str = str(e)
                    if retry < 2 and any(kw in err_str for kw in ("ERR_NAME_NOT_RESOLVED","ERR_NETWORK_CHANGED","ERR_CONNECTION","Connection closed","Target closed","Page crashed")):
                        retry += 1
                        logger.warning(f"@{acc['username']}: retry {retry}/2: {err_str[:60]}")
                        if page: await page.close()
                        if ctx: await ctx.close()
                        ctx = await _make_context(browser, cookies_json, proxy)
                        page = await ctx.new_page()
                        await asyncio.sleep(3)
                    else:
                        all_errors.append(f"@{acc['username']}: {err_str}")
                        logger.error(f"@{acc['username']}: 失败: {err_str[:200]}")
                        break
            # Fresh page per account + human-like delay to avoid X rate-limit / page crashes
            try:
                if page: await page.close()
            except Exception:
                pass
            page = await ctx.new_page()
            await asyncio.sleep(2 + random.random() * 3)
            task_update(task_id, progress_current=i + 1)
    finally:
        from scraper.playwright_scraper import BROWSER_MODE as _bm
        # CDP mode: only close the page; NEVER close user's real context/browser
        objs = [page] if _bm == "cdp" else [page, ctx, browser]
        for obj in objs:
            if obj:
                try: await obj.close()
                except Exception: pass
        # CDP mode: stop the per-run driver (fresh one). Otherwise keep global singleton.
        if _bm == "cdp" and pw:
            try: await pw.stop()
            except Exception: pass

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Save all errors (no truncation)
    error_str = "; ".join(all_errors) if all_errors else None
    # Manual tasks stay completed; scheduled tasks stay active
    new_status = "completed" if task["schedule_type"] == "manual" else "active"
    task_update(task_id, status=new_status, last_run_at=now,
                progress_current=total, error_message=error_str,
                run_count=task["run_count"] + 1)
    task_run_finish(run_id, new_total, error_str,
                    accounts_total=total,
                    accounts_success=accounts_success,
                    error_count=len([e for e in all_errors if e]))
    # If scheduled, calculate next run
    if task["schedule_type"] == "interval" and task.get("interval_minutes"):
        next_dt = datetime.now() + timedelta(minutes=task["interval_minutes"])
        task_update(task_id, next_run_at=next_dt.strftime("%Y-%m-%d %H:%M:%S"))
    # Re-schedule to ensure APScheduler job stays fresh (prevents scheduler freeze)
    if task["schedule_type"] != "manual":
        updated_task = next((t for t in task_list() if t["id"] == task_id), None)
        if updated_task:
            _schedule_task(updated_task)

    logger.info(f"任务执行完成: #{task_id} {task['name']}, 成功{accounts_success}/{total}账户, 新增{new_total}条, 错误{len([e for e in all_errors if e])}个")
    LogContext.clear()


@app.on_event("startup")
async def startup():
    pass  # scheduling moved to lifespan


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8765, reload=True)
