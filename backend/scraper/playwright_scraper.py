"""
X/Twitter scraper via Playwright async API + system Chrome.
All Playwright interactions use async/await to coexist with FastAPI's event loop.
"""
import json, logging, re, time, random, os, asyncio
from datetime import datetime
from typing import Optional

try:
    from logging_config import get_logger
    logger = get_logger("playwright")
except ImportError:
    logger = logging.getLogger(__name__)

PLAYWRIGHT_AVAILABLE = False

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    logger.error("Playwright 未安装: pip install playwright && playwright install chromium")

BROWSER_MODE = os.environ.get("BROWSER_MODE", "chrome")
CDP_PORT = int(os.environ.get("CDP_PORT", "9222"))

CHROME_USER_DATA = os.environ.get(
    "CHROME_USER_DATA",
    r"C:\Users\Administrator\AppData\Local\Google\Chrome\User Data"
)
CHROME_PROFILE = os.environ.get("CHROME_PROFILE", "Default")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

VIEWPORTS = [{"width": 1920, "height": 1080}, {"width": 1440, "height": 900}, {"width": 1366, "height": 768}]


async def _kill_zombie_chrome():
    """Kill headless Chrome processes with no window title (zombies). Uses async subprocess to avoid blocking the event loop."""
    try:
        proc = await asyncio.create_subprocess_exec(
            'powershell', '-NoProfile', '-Command',
            "Get-Process chrome -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowTitle -eq '' } | Stop-Process -Force",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
        killed = stdout.decode(errors='ignore').strip()
        if killed:
            logger.info(f"Killed zombie Chrome processes: {killed}")
    except Exception as e:
        logger.debug(f"taskkill zombies: {e}")


async def _kill_zombie_driver():
    """Kill leftover Playwright driver (Node.js) processes that didn't shut down properly.
    These are node.exe children spawned by Playwright, identified by 'playwright' in the command line."""
    try:
        proc = await asyncio.create_subprocess_exec(
            'powershell', '-NoProfile', '-Command',
            "Get-CimInstance Win32_Process -Filter \"Name='node.exe'\" | Where-Object { $_.CommandLine -match 'playwright' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
        out = stdout.decode(errors='ignore').strip()
        if out:
            logger.info(f"Killed zombie Playwright drivers: {out}")
    except Exception as e:
        logger.debug(f"kill playwright zombies: {e}")


ANTI_DETECT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => false });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN','en-US','en'] });
Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
const origQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (args) => {
    if (args.name === 'notifications') return Promise.resolve({ state: Notification.permission });
    return origQuery(args);
};
window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {}, app: {} };
(() => { const i = document.createElement('iframe'); i.style.display = 'none'; document.body.appendChild(i); const p = i.contentWindow; document.body.removeChild(i); })();
"""


async def check_account_exists(username: str) -> bool:
    """Check if a Twitter account exists. Returns False if emptyState found or page title missing."""
    username = username.strip().lstrip("@")
    pw = browser = page = None
    try:
        pw, browser = await _make_browser()
        page = await browser.new_page()
        await page.goto(f"https://x.com/{username}", wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(2000)
        # Check 1: emptyState means suspended/deleted
        empty = await page.query_selector('[data-testid="emptyState"]')
        if empty:
            return False
        # Check 2: no page title means redirect to homepage (account doesn't exist)
        title = await page.title()
        if not title or title.strip() == "X":
            return False
        return True
    except Exception as e:
        logger.warning(f"check_account_exists(@{username}): {e}")
        return True  # Can't verify, don't block
    finally:
        for obj, method in [(page, 'close'), (browser if BROWSER_MODE != 'cdp' and browser else None, 'close'), (pw, 'stop')]:
            if obj:
                try:
                    await getattr(obj, method)()
                except Exception as e:
                    logger.debug(f"check_account_exists cleanup {method}(): {e}")
        await _kill_zombie_chrome()


async def _make_browser(pw=None):
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright 未安装")

    if pw is None:
        pw = await async_playwright().start()

    mode = BROWSER_MODE
    if mode == "cdp":
        browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{CDP_PORT}")
        return pw, browser

    headless = (mode != "chrome-visible")
    args = [
        "--disable-blink-features=AutomationControlled",
        "--no-first-run", "--no-default-browser-check",
        "--disable-background-networking",
    ]
    if headless:
        args.append("--headless=new")

    browser = await pw.chromium.launch(headless=headless, args=args)
    return pw, browser


async def _make_context(browser, cookies_json: Optional[str] = None, proxy: Optional[str] = None):
    """Create browser context with cookies and anti-detection."""
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport=random.choice(VIEWPORTS),
        locale="en-US",
        timezone_id="America/New_York",
        geolocation={"longitude": -73.9857, "latitude": 40.7484},
        permissions=[],
        color_scheme="light",
        proxy={"server": proxy} if proxy else None,
    )
    await context.add_init_script(ANTI_DETECT_SCRIPT)

    if cookies_json:
        try:
            cookies = json.loads(cookies_json)
            cookie_list = cookies if isinstance(cookies, list) else cookies.get("cookies", [])
            if cookie_list:
                page = await context.new_page()
                await page.goto("https://x.com/", wait_until="commit", timeout=10000)
                await context.add_cookies(cookie_list)
                await page.close()
                logger.info(f"Injected {len(cookie_list)} cookies")
        except Exception as e:
            logger.warning(f"Cookie 注入失败: {e}")

    return context


async def validate_login(cookies_json: str, proxy: Optional[str] = None) -> dict:
    """Validate X login by checking home page."""
    if not PLAYWRIGHT_AVAILABLE:
        return {"valid": False, "error": "Playwright 未安装"}

    pw = None
    browser = None
    context = None
    try:
        pw, browser = await _make_browser()
        context = await _make_context(browser, None, proxy)
        page = await context.new_page()
        await page.goto("https://x.com/", wait_until="commit", timeout=15000)

        try:
            cookies = json.loads(cookies_json)
            if isinstance(cookies, list):
                await page.evaluate(
                    "(cs) => cs.forEach(c => { document.cookie = c.name + '=' + encodeURIComponent(c.value) + '; domain=' + c.domain + '; path=' + (c.path||'/') })",
                    cookies,
                )
        except Exception:
            pass

        await page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(4000)

        checks = {
            "composer": await page.query_selector('[data-testid="tweetTextarea_0"]'),
            "timeline": await page.query_selector('[data-testid="primaryColumn"]'),
            "sidebar": await page.query_selector('[data-testid="SideNav_AccountSwitcher_Button"]'),
            "post_btn": await page.query_selector('[data-testid="tweetButton"]'),
        }
        if any(checks.values()) or "home" in page.url:
            return {"valid": True, "message": "凭证有效"}
        if "login" in page.url.lower():
            return {"valid": False, "error": "凭证已过期"}
        return {"valid": False, "error": f"无法确认登录状态 (URL:{page.url[:60]})"}
    except Exception as e:
        return {"valid": False, "error": str(e)[:100]}
    finally:
        for obj, method in [(context, 'close'), (browser if BROWSER_MODE != 'cdp' else None, 'close'), (pw, 'stop')]:
            if obj:
                try:
                    await getattr(obj, method)()
                except Exception as e:
                    logger.debug(f"validate_login cleanup {method}(): {e}")
        await _kill_zombie_chrome()


async def fetch_user_timeline(username: str, max_tweets: int = 5,
                              cookies_json: Optional[str] = None,
                              proxy: Optional[str] = None,
                              last_tweet_id: Optional[str] = None,
                              existing_ids: Optional[set] = None,
                              filters: Optional[dict] = None) -> list:
    if not PLAYWRIGHT_AVAILABLE:
        return []
    filters = filters or {}
    username = username.strip().lstrip("@")
    tweets = []
    existing = existing_ids or set()
    pw = None
    browser = None
    context = None

    try:
        pw, browser = await _make_browser()
        context = await _make_context(browser, cookies_json, proxy)
        page = await context.new_page()

        url = f"https://x.com/{username}"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(1000)

        empty = await page.query_selector('[data-testid="emptyState"]')
        if empty:
            logger.warning(f"@{username}: 账号不存在或为私密账号")
            return []

        try:
            await page.wait_for_selector('[data-testid="tweet"]', timeout=15000)
        except Exception:
            logger.warning(f"@{username}: 未找到推文")
            return []

        seen_ids = set(existing)
        hit_boundary = False
        scroll_attempts = 0
        max_scrolls = min(60, max_tweets * 2)
        no_new_count = 0

        while len(tweets) < max_tweets and scroll_attempts < max_scrolls and not hit_boundary:
            articles = await page.query_selector_all('[data-testid="tweet"]')
            new_found = 0

            for article in articles:
                try:
                    td = await _extract_tweet(article, username)
                    if not td:
                        continue
                    tid = td["tweet_id"]
                    if tid in seen_ids:
                        continue

                    if last_tweet_id:
                        try:
                            if int(tid) <= int(last_tweet_id):
                                hit_boundary = True
                                break
                        except (ValueError, TypeError):
                            pass

                    if tid in existing:
                        hit_boundary = True
                        break

                    if _check_filters(td, filters):
                        continue

                    seen_ids.add(tid)
                    tweets.append(td)
                    new_found += 1
                    if len(tweets) >= max_tweets:
                        break
                except Exception:
                    pass

            if hit_boundary:
                break

            if new_found == 0:
                no_new_count += 1
                if no_new_count >= 5:
                    break
            else:
                no_new_count = 0

            await page.evaluate("window.scrollBy({top: window.innerHeight * 0.75, behavior: 'smooth'})")
            await page.wait_for_timeout(800 + random.randint(300, 700))
            scroll_attempts += 1

        reason = "边界" if hit_boundary else ("已满" if len(tweets) >= max_tweets else "无新内容")
        logger.info(f"@{username}: 采集完成 ({reason}), 新增 {len(tweets)} 条")
    except Exception as e:
        logger.error(f"Playwright error @{username}: {e}")
        raise
    finally:
        for obj, method in [(context, 'close'), (browser if BROWSER_MODE != 'cdp' else None, 'close'), (pw, 'stop')]:
            if obj:
                try:
                    await getattr(obj, method)()
                except Exception as e:
                    logger.debug(f"fetch_user_timeline cleanup {method}(): {e}")
        await _kill_zombie_chrome()

    return tweets[:max_tweets]


async def fetch_timeline_with_page(page, username: str, max_tweets: int = 5,
                                     last_tweet_id: Optional[str] = None,
                                     existing_ids: Optional[set] = None,
                                     filters: Optional[dict] = None) -> list:
    """Fetch timeline using an existing page (navigates to username, no tab overhead)."""
    filters = filters or {}
    username = username.strip().lstrip("@")
    tweets = []
    existing = existing_ids or set()

    await page.goto(f"https://x.com/{username}", wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(3500)

    empty = await page.query_selector('[data-testid="emptyState"]')
    if empty:
        logger.warning(f"@{username}: 账号不存在或为私密账号")
        return []

    try:
        await page.wait_for_selector('[data-testid="tweet"]', timeout=15000)
    except Exception:
        logger.warning(f"@{username}: 未找到推文")
        return []

    seen_ids = set(existing)
    hit_boundary = False
    scroll_attempts = 0
    max_scrolls = min(60, max_tweets * 2)
    no_new_count = 0

    while len(tweets) < max_tweets and scroll_attempts < max_scrolls and not hit_boundary:
        articles = await page.query_selector_all('[data-testid="tweet"]')
        new_found = 0
        for article in articles:
            try:
                td = await _extract_tweet(article, username)
                if not td: continue
                tid = td["tweet_id"]
                if tid in seen_ids: continue
                if last_tweet_id:
                    try:
                        if int(tid) <= int(last_tweet_id):
                            hit_boundary = True; break
                    except (ValueError, TypeError):
                        pass
                if tid in existing:
                    hit_boundary = True; break
                if _check_filters(td, filters): continue
                seen_ids.add(tid)
                tweets.append(td)
                new_found += 1
                if len(tweets) >= max_tweets: break
            except Exception:
                pass
        if hit_boundary: break
        if new_found == 0:
            no_new_count += 1
            if no_new_count >= 5: break
        else:
            no_new_count = 0
        await page.evaluate("window.scrollBy({top: window.innerHeight * 0.75, behavior: 'smooth'})")
        await page.wait_for_timeout(1800 + random.randint(500, 2000))
        scroll_attempts += 1

    return tweets[:max_tweets]


async def _extract_tweet(article, username: str) -> Optional[dict]:
    try:
        link = await article.query_selector('a[href*="/status/"]')
        if not link:
            return None
        href = await link.get_attribute("href") or ""
        m = re.search(r'/status/(\d+)', href)
        if not m:
            return None
        tweet_id = m.group(1)

        text = ""
        text_el = await article.query_selector('[data-testid="tweetText"]')
        if text_el:
            spans = await text_el.query_selector_all('span')
            text = "".join([(await s.inner_text() or "") for s in spans]) if spans else (await text_el.inner_text() or "")

        time_el = await article.query_selector("time")
        created_at = await time_el.get_attribute("datetime") if time_el else None

        likes = await _parse_stat(article, "like")
        retweets = (await _parse_stat(article, "retweet")) or (await _parse_stat(article, "repost"))
        replies = await _parse_stat(article, "reply")
        views = await _parse_stat(article, "view")

        pin_el = await article.query_selector('[data-testid="socialContext"]')
        is_pinned = "pin" in ((await pin_el.inner_text()).lower() if pin_el else "")

        media_urls = []
        for img in await article.query_selector_all('[data-testid="tweetPhoto"] img'):
            src = await img.get_attribute("src") or ""
            if src and "profile" not in src.lower():
                media_urls.append(src)
        for vid in await article.query_selector_all('video'):
            src = await vid.get_attribute("src") or await vid.get_attribute("poster") or ""
            if src:
                media_urls.append(src)

        tweet_type = await _detect_tweet_type(article)

        # Extract retweet/quote original author from link in the article
        retweet_author = ""
        if tweet_type in ("retweet", "quote"):
            links = await article.query_selector_all('a[href^="/"]')
            for lk in links[:15]:
                href = (await lk.get_attribute("href")) or ""
                href = href.rstrip("/").split("?")[0]
                if not href or href == f"/{username}" or href.startswith("/i/") or href.startswith("/hashtag/"):
                    continue
                # Extract username from href like "/username" or "/username/status/123"
                parts = [p for p in href.split("/") if p and not p.isdigit() and p != "status"]
                if parts and len(parts[-1]) > 1 and "/" not in parts[-1]:
                    retweet_author = parts[-1].lstrip("@")
                    break

        return {
            "tweet_id": tweet_id, "author_username": username,
            "author_display_name": username, "text": text.strip(),
            "media_urls": media_urls, "quoted_tweet_id": None,
            "likes": likes, "retweets": retweets, "replies": replies,
            "views": views, "bookmarks": 0, "language": None,
            "is_pinned": is_pinned, "is_reply": tweet_type == "reply",
            "is_article": False,
            "url": f"https://x.com/{username}/status/{tweet_id}",
            "tweet_type": tweet_type, "retweet_author": retweet_author, "created_at": created_at,
        }
    except Exception:
        return None


async def _detect_tweet_type(article) -> str:
    """Detect tweet type from DOM structure."""
    try:
        ctx = await article.query_selector('[data-testid="socialContext"]')
        ctx_text = ((await ctx.inner_text()) or "") if ctx else ""
        ctx_lower = ctx_text.lower()

        # Quote detection — only use [data-testid="quote"] for now
        has_quote = bool(await article.query_selector('[data-testid="quote"]'))

        # Retweet detection
        retweet_kw = ["repost", "retweet", "reposted"]
        is_retweet = ctx and any(kw in ctx_lower for kw in retweet_kw)

        if not is_retweet and ctx:
            cjk_kw = ["已转帖", "已轉帖", "转发了", "轉發了", "リツイート"]
            is_retweet = any(kw in ctx_text for kw in cjk_kw)

        if not is_retweet and ctx:
            spans = await article.query_selector_all('span')
            for span in spans[:10]:
                txt = (await span.inner_text()) or ""
                if any(kw in txt.lower() for kw in retweet_kw) or any(kw in txt for kw in ["已转帖","转发了"]):
                    is_retweet = True
                    break

        if ctx_text:
            logger.info(f"_detect: ctx='{ctx_text[:50]}' quote={has_quote} retweet={is_retweet}")

        if "replying to" in ctx_lower or "reply" in ctx_lower or "已回复" in ctx_text:
            return "reply"
        elif has_quote:
            return "quote"
        elif is_retweet:
            return "retweet"
        else:
            return "original"
    except Exception:
        return "unknown"


def _check_filters(tweet: dict, filters: dict) -> bool:
    """Return True if tweet should be SKIPPED."""
    if not filters:
        return False
    allowed_types = filters.get("tweet_types")
    if allowed_types:
        if isinstance(allowed_types, str):
            allowed_types = [allowed_types]
        if tweet.get("tweet_type", "unknown") not in allowed_types:
            return True
    if filters.get("min_likes") and tweet.get("likes", 0) < filters["min_likes"]:
        return True
    if filters.get("min_retweets") and tweet.get("retweets", 0) < filters["min_retweets"]:
        return True
    if filters.get("min_views") and tweet.get("views", 0) < filters["min_views"]:
        return True
    if filters.get("media_only"):
        media = tweet.get("media_urls", [])
        if isinstance(media, str):
            try:
                media = json.loads(media)
            except Exception:
                media = []
        if not media:
            return True
    return False


async def _parse_stat(article, stat_type: str) -> int:
    selector = f'[data-testid="{stat_type}"]'
    for el in await article.query_selector_all(selector):
        label = await el.get_attribute("aria-label") or ""
        nums = re.findall(r'[\d,]+', label)
        if nums:
            return int(nums[-1].replace(",", ""))
    return 0


async def launch_x_login(proxy: Optional[str] = None) -> dict:
    """Launch visible Chrome for manual X login, extract cookies."""
    if not PLAYWRIGHT_AVAILABLE:
        return {"error": "Playwright 未安装"}

    pw = None
    browser = None
    context = None
    try:
        await _kill_zombie_chrome()
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            channel="chrome", headless=False,
            args=["--disable-blink-features=AutomationControlled", "--start-maximized", "--no-sandbox"],
        )
        context = await browser.new_context(viewport={"width": 1440, "height": 900}, locale="zh-CN")
        await context.add_init_script(ANTI_DETECT_SCRIPT)
        page = await context.new_page()
        await page.goto("https://x.com/login", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)

        logger.info("Waiting for manual X login (5 min timeout)...")
        logged_in = False
        for _ in range(150):  # 5 min, check every 2s
            try:
                if await page.query_selector('[data-testid="tweetTextarea_0"]') or \
                   await page.query_selector('[data-testid="SideNav_AccountSwitcher_Button"]'):
                    logged_in = True
                    break
            except Exception:
                pass
            await page.wait_for_timeout(2000)

        if not logged_in:
            return {"error": "登录超时（5分钟），请重试"}

        screen_name = "unknown"
        try:
            nav = await page.query_selector('[data-testid="SideNav_AccountSwitcher_Button"]')
            if nav:
                text = await nav.inner_text() or ""
                m = re.search(r'@(\w+)', text)
                if m:
                    screen_name = m.group(1)
        except Exception:
            pass

        cookies = await context.cookies()
        cookies_list = [{"name": c["name"], "value": c["value"], "domain": c.get("domain", ""),
                         "path": c.get("path", "/"), "httpOnly": c.get("httpOnly", False),
                         "secure": c.get("secure", False)} for c in cookies]
        logger.info(f"Login OK: @{screen_name}, {len(cookies_list)} cookies")
        return {"cookies_json": json.dumps(cookies_list, ensure_ascii=False), "label": f"@{screen_name} 的 Cookie"}
    except Exception as e:
        return {"error": str(e)[:200]}
    finally:
        if context: await context.close()
        if browser: await browser.close()
        if pw: await pw.stop()
