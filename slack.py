from typing import Dict, Any
import requests
import structlog
from config import Settings


logger = structlog.get_logger()

class SlackAPI:
    """Class for interacting with Slack API to send messages"""

    def __init__(self) -> None:
        self.settings: Settings = Settings()
        self.base_url: str = "https://slack.com/api/chat.postMessage"
        self.headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.settings.SLACK_BOT_TOKEN}"
        }
        self.session: requests.Session = requests.Session()

    async def send_message(self, channel: str, text: str) -> Dict[str, Any]:
        """
        Send a message to a Slack channel

        Args:
            channel: The channel to send the message to
            text: The message text to send

        Returns:
            Dict containing the Slack API response

        Raises:
            requests.RequestException: If the request to Slack fails
        """
        payload = {
            "text": text,
            "channel": channel
        }

        try:
            response = self.session.post(
                url=self.base_url,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            response_data = response.json()

            if response.status_code == 200 and response_data.get("ok"):
                logger.info(
                    "slack.message_sent",
                    channel=channel,
                    message_ts=response_data.get("ts", "null")
                )
            else:
                error = response_data.get("error", "Unknown error")
                logger.error(
                    "slack.message_failed",
                    channel=channel,
                    error=error,
                    status_code=response.status_code
                )

            return response_data

        except requests.RequestException as e:
            logger.error(
                "slack.request_failed",
                channel=channel,
                error=str(e)
            )
            raise
