"""
Centralized logging configuration for X Scraper.
Features: structured JSON-lines format, correlation IDs, traceback capture, log persistence.
"""
import logging, sys, os, json, traceback, threading, asyncio
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
LOG_FILE = os.path.join(LOG_DIR, "app.log.jsonl")  # JSON lines for structured readability


class JsonFormatter(logging.Formatter):
    """Structured JSON log formatter with correlation context."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Inject correlation context
        task_id = getattr(record, "task_id", None)
        run_id = getattr(record, "run_id", None)
        if task_id:
            payload["task"] = task_id
        if run_id:
            payload["run"] = run_id

        if record.exc_info and record.exc_info[1]:
            payload["error_type"] = type(record.exc_info[1]).__name__
            payload["traceback"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


class LogContext:
    """Thread-local context for correlation IDs. Set before task execution, clear after."""
    _local = threading.local()

    @classmethod
    def set(cls, task_id: int = None, run_id: int = None):
        cls._local.task_id = task_id
        cls._local.run_id = run_id

    @classmethod
    def clear(cls):
        cls._local.task_id = None
        cls._local.run_id = None

    @classmethod
    def get_task_id(cls) -> int | None:
        return getattr(cls._local, "task_id", None)

    @classmethod
    def get_run_id(cls) -> int | None:
        return getattr(cls._local, "run_id", None)


class ContextFilter(logging.Filter):
    """Inject correlation context into log records."""

    def filter(self, record):
        record.task_id = LogContext.get_task_id()
        record.run_id = LogContext.get_run_id()
        return True


class DbLogHandler(logging.Handler):
    """Buffered log handler that persists to SQLite logs table."""

    _instance = None
    _flush_interval = 2  # seconds

    def __init__(self):
        super().__init__()
        self._buffer = []
        self._lock = threading.Lock()
        self._running = True
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()
        DbLogHandler._instance = self

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = DbLogHandler()
        return cls._instance

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            with self._lock:
                self._buffer.append(json.loads(msg))
        except Exception:
            self.handleError(record)

    def _flush_loop(self):
        while self._running:
            try:
                threading.Event().wait(self._flush_interval)
                self._flush()
            except Exception:
                pass

    def _flush(self):
        from database import log_batch_insert
        with self._lock:
            if not self._buffer:
                return
            batch = self._buffer[:]
            self._buffer = []
        try:
            log_batch_insert(batch)
        except Exception:
            pass  # Don't crash logger on DB errors

    def close(self):
        self._running = False
        self._flush()
        super().close()


def setup_logging(log_level: str = "INFO"):
    """Initialize structured logging. Call once at startup."""
    os.makedirs(LOG_DIR, exist_ok=True)

    # Reset root logger
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)

    level = getattr(logging, log_level.upper(), logging.INFO)
    root.setLevel(level)

    # File handler — JSON lines
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(JsonFormatter())
    root.addHandler(fh)

    # Stream handler — plain text for console
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(level)
    sh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    root.addHandler(sh)

    # Database handler — buffered batch insert
    try:
        dbh = DbLogHandler()
        dbh.setLevel(level)
        dbh.setFormatter(JsonFormatter())
        root.addHandler(dbh)
    except Exception:
        pass

    # Context filter for task/run correlation
    ctx_filter = ContextFilter()
    for handler in root.handlers:
        handler.addFilter(ctx_filter)

    # Bufferless logger for noisy modules
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the standard name format."""
    return logging.getLogger(name)
