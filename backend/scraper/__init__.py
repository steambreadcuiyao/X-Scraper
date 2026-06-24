"""Unified scraper runner — Playwright async API only."""
import sys, os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .playwright_scraper import fetch_user_timeline, validate_login, launch_x_login, PLAYWRIGHT_AVAILABLE


async def run_scraper(account: dict, max_tweets: int = 5,
                      cookies_json: str = None, proxy: str = None,
                      filters: dict = None) -> dict:
    from database import tweet_insert, account_update, tweet_search as _ts, get_last_tweet_id, get_existing_tweet_ids

    username = account["username"]
    result = {"account_id": account["id"], "username": username, "channel": "playwright", "new_tweets": 0, "errors": []}

    if not PLAYWRIGHT_AVAILABLE:
        result["errors"].append("Playwright 未安装")
        return result

    last_tweet_id = get_last_tweet_id(account["id"])
    existing_ids = set(get_existing_tweet_ids(account["id"], limit=300))

    try:
        tweets = await fetch_user_timeline(
            username, max_tweets, cookies_json, proxy,
            last_tweet_id=last_tweet_id, existing_ids=existing_ids, filters=filters,
        )
    except Exception as e:
        err_str = str(e)
        # Re-raise browser/connection crashes so outer retry can handle
        if "Connection closed" in err_str or "Target closed" in err_str:
            raise
        result["errors"].append(f"采集异常: {err_str[:200]}")
        return result

    for t in tweets:
        t["account_id"] = account["id"]
        try:
            if tweet_insert(t):
                result["new_tweets"] += 1
        except Exception as e:
            result["errors"].append(f"入库失败: {str(e)[:100]}")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = _ts(author=username, limit=1)["total"]
    account_update(account["id"], last_scraped_at=now, tweet_count=total)

    return result
