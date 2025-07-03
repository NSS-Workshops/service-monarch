""" Configuration settings for the Monarch service """
import os
from pydantic import BaseModel, HttpUrl

class Settings(BaseModel):
    """ Configuration settings for the Monarch service """
    VALKEY_HOST: str = os.getenv("VALKEY_HOST", "localhost")
    VALKEY_PORT: int = 6379
    VALKEY_DB: int = 0
    GITHUB_API_URL: HttpUrl = "https://api.github.com"
    GITHUB_TOKEN: str = os.getenv("GH_PAT")
    GITHUB_RATE_LIMIT_PAUSE: int = 5
    REPO_MIGRATION_PAUSE: int = 10
    PROMETHEUS_PORT: int = 8080
    # Update template directory path to account for the new directory structure
    TEMPLATE_DIR: str = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    SLACK_BOT_TOKEN: str = os.getenv("SLACK_BOT_TOKEN")

    class Config:
        env_file = ".env"