import time
from dagster import ConfigurableResource
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from .utils import SEC_USER_AGENT, SEC_BASE_URL

class SECClient(ConfigurableResource):
    """
    A Dagster Resource to handle connections to the SEC EDGAR database.
    Includes rate limiting and automatic retries.
    """
    def _get_headers(self):
        return {
            "User-Agent": SEC_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_content(self, url: str, as_bytes: bool = False):
        """
        Fetches content from a URL with rate limiting logic.
        SEC Limit: 10 requests per second.
        """
        # Sleep to be a polite citizen (simple rate limiting)
        # In a production app, you might use a token bucket algorithm.
        time.sleep(0.15) 

        print(f"Fetching: {url}")
        
        with httpx.Client(headers=self._get_headers(), timeout=10.0, follow_redirects=True) as client:
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