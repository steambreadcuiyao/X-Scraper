"""
Worker process for Playwright scraping. Runs in a clean subprocess to avoid
uvicorn event loop interference on Windows.

Usage: python scraper_worker.py <input_json_file> <output_json_file>
Input JSON contains task config + account list + filters.
Output JSON contains list of {account_index, status, tweets, error}.
"""
import asyncio, sys, json, os, random, traceback, datetime

# Write startup debug to a log file
_debug_log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "worker_debug.log")
with open(_debug_log, "a", encoding="utf-8") as _f:
    _f.write(f"\n[{datetime.datetime.now()}] Worker started, args={sys.argv}\n")
    _f.write(f"Python: {sys.executable}\n")
    _f.write(f"cwd: {os.getcwd()}\n")
    _f.flush()

# Must be set before any asyncio operations on Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("BROWSER_MODE", "chrome-visible")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
]
VIEWPORTS = [{"width": 1920, "height": 1080}, {"width": 1440, "height": 900}, {"width": 1366, "height": 768}]


async def scrape_account(pw, account, config, existing_ids):
    """Scrape one account. Returns {username, tweets, error}."""
    username = account["username"]
    max_tweets = config.get("max_tweets_per_run", 5)
    cookies_json = config.get("cookies_json")
    proxy = config.get("proxy")
    filters = config.get("filters", {})
    last_tweet_id = account.get("last_tweet_id")

    browser = None
    ctx = None
    page = None
    try:
        mode = os.environ.get("BROWSER_MODE", "chrome-visible")
        headless = (mode != "chrome-visible")
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--no-first-run", "--no-default-browser-check",
            "--disable-background-networking", "--no-sandbox",
        ]
        if headless:
            launch_args.append("--headless=new")

        browser = await pw.chromium.launch(headless=headless, args=launch_args)
        ctx = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport=random.choice(VIEWPORTS),
            locale="en-US",
        )
        if cookies_json:
            try:
                cookies = json.loads(cookies_json) if isinstance(cookies_json, str) else cookies_json
                await ctx.add_cookies(cookies)
            except Exception:
                pass
        if proxy:
            await ctx.route("**/*", lambda route: route.continue_())

        page = await ctx.new_page()
        from scraper.playwright_scraper import fetch_timeline_with_page
        tweets = await fetch_timeline_with_page(
            page, username, max_tweets,
            last_tweet_id=last_tweet_id,
            existing_ids=set(existing_ids),
            filters=filters
        )
        return {"username": username, "tweets": tweets, "error": None}
    except Exception as e:
        return {"username": username, "tweets": [], "error": str(e)}
    finally:
        for obj in [page, ctx, browser]:
            if obj:
                try: await obj.close()
                except Exception: pass


async def main():
    if len(sys.argv) < 3:
        print(json.dumps({"ok": False, "error": "usage: scraper_worker.py <input> <output>"}))
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    config = data.get("config", {})
    accounts = data.get("accounts", [])

    from playwright.async_api import async_playwright

    results = []
    try:
        pw = await async_playwright().start()
    except Exception as e:
        results = [{"username": a["username"], "tweets": [], "error": f"Driver failed: {e}"} for a in accounts]
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump({"ok": True, "results": results}, f, ensure_ascii=False)
        return

    try:
        for acc in accounts:
            result = await scrape_account(pw, acc, config, acc.get("existing_ids", []))
            results.append(result)
    finally:
        try: await pw.stop()
        except Exception: pass

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({"ok": True, "results": results}, f, ensure_ascii=False)


if __name__ == "__main__":
    asyncio.run(main())
