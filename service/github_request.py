from typing import Any, Callable, Dict, Optional
import json
import time
import requests
import structlog
from config import Settings


logger = structlog.get_logger()

class GithubRequest(object):
    """ A class to handle GitHub API requests with retry logic and structured logging."""
    def __init__(self) -> None:
        self.session: requests.Session = requests.Session()
        self.settings: Settings = Settings()

        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/vnd.github+json",
            "User-Agent": "nss/ticket-migrator",
            "X-GitHub-Api-Version": "2022-11-28",
            "Authorization": f'Bearer {self.settings.GITHUB_TOKEN}'
        })

    def get(self, url: str) -> requests.Response:
        """ Make a GET request to the specified URL with retry logic."""
        logger.info("github_request.get", url=url)
        return self.request_with_retry(lambda: self.session.get(
            url=url,
            headers=self.session.headers,
            timeout=10)
        )

    def put(self, url: str, data: Dict[str, Any]) -> requests.Response:
        """ Make a PUT request to the specified URL with retry logic."""
        logger.info("github_request.put", url=url)
        json_data = json.dumps(data)
        return self.request_with_retry(lambda: self.session.put(
            url=url,
            data=json_data,
            headers=self.session.headers,
            timeout=10)
        )

    def post(self, url: str, data: Dict[str, Any]) -> Optional[requests.Response]:
        """ Make a POST request to the specified URL with retry logic."""
        logger.info("github_request.post", url=url)
        json_data = json.dumps(data)

        try:
            result = self.request_with_retry(lambda: self.session.post(
                url=url,
                data=json_data,
                headers=self.session.headers,
                timeout=10)
            )
            return result

        except TimeoutError:
            print("Request timed out. Trying next...")

        except ConnectionError:
            print("Request timed out. Trying next...")

        return None

    def request_with_retry(self, request: Callable[[], requests.Response]) -> requests.Response:
        """ Retry the request if it fails with a 403 status code."""
        retry_after_seconds: int = 1800
        number_of_retries: int = 0

        response = request()

        while response.status_code == 403 and number_of_retries <= 10:
            number_of_retries += 1
            self.sleep_with_countdown(retry_after_seconds)
            response = request()

        return response

    def sleep_with_countdown(self, countdown_seconds: int) -> None:
        """ Sleep for the specified number of seconds, with a countdown display."""
        ticks: int = countdown_seconds * 2
        for count in range(ticks, -1, -1):
            if count:
                time.sleep(0.5)
