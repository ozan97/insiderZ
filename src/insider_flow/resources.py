import threading
import time
from dagster import ConfigurableResource
import httpx
from pydantic import PrivateAttr
from tenacity import retry, stop_after_attempt, wait_exponential
from .utils import SEC_USER_AGENT, SEC_BASE_URL

# Global rate limiter: allows at most 8 requests per second across all threads.
# Each thread must acquire() before making a request. A background timer
# releases one permit every 1/8 s, capping at 8 permits (burst size).
_rate_lock = threading.Lock()
_rate_permits = 8
_MAX_PERMITS = 8
_REFILL_INTERVAL = 1.0 / _MAX_PERMITS  # 0.125 s


def _refill_permits():
    """Daemon timer that continuously refills one permit per interval."""
    global _rate_permits
    while True:
        time.sleep(_REFILL_INTERVAL)
        with _rate_lock:
            if _rate_permits < _MAX_PERMITS:
                _rate_permits += 1


# Start the refill thread once on module load
_refill_thread = threading.Thread(target=_refill_permits, daemon=True)
_refill_thread.start()


def _acquire_rate_permit():
    """Block until a rate-limit permit is available."""
    global _rate_permits
    while True:
        with _rate_lock:
            if _rate_permits > 0:
                _rate_permits -= 1
                return
        time.sleep(0.02)  # brief spin before retrying


class SECClient(ConfigurableResource):
    """
    A Dagster Resource to handle connections to the SEC EDGAR database.
    Includes rate limiting and automatic retries.
    """
    _http_client: httpx.Client | None = PrivateAttr(default=None)
    _client_lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def _get_headers(self):
        return {
            "User-Agent": SEC_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }

    def _ensure_client(self) -> httpx.Client:
        """Lazily create a single, reusable httpx Client (thread-safe)."""
        if self._http_client is None or self._http_client.is_closed:
            with self._client_lock:
                if self._http_client is None or self._http_client.is_closed:
                    self._http_client = httpx.Client(
                        headers=self._get_headers(),
                        timeout=10.0,
                        follow_redirects=True,
                    )
        return self._http_client

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_content(self, url: str, as_bytes: bool = False):
        """
        Fetches content from a URL with rate limiting logic.
        SEC Limit: 10 requests per second.
        Uses a global token-bucket limiter safe for multi-threaded use.
        """
        _acquire_rate_permit()

        client = self._ensure_client()
        response = client.get(url)
        response.raise_for_status()
        return response.content if as_bytes else response.text

    def get_daily_index_url(self, date_obj) -> str:
        """
        Constructs the URL for the daily master index file.
        Format: https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTRx/master.YYYYMMDD.idx
        """
        year = date_obj.year
        qtr = (date_obj.month - 1) // 3 + 1
        date_str = date_obj.strftime("%Y%m%d")
        
        return f"{SEC_BASE_URL}/edgar/daily-index/{year}/QTR{qtr}/master.{date_str}.idx"