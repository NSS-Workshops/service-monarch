""" Configuration settings for the Monarch service """
import os
from pydantic import BaseModel, HttpUrl

class Settings(BaseModel):
    """ Configuration settings for the Monarch service """
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    GITHUB_API_URL: HttpUrl = "https://api.github.com"
    GITHUB_TOKEN: str = os.getenv("GITHUB_PAT")
    GITHUB_RATE_LIMIT_PAUSE: int = 5
    REPO_MIGRATION_PAUSE: int = 300
    TEMPLATE_DIR: str = os.path.dirname(os.path.realpath(__file__))
    SLACK_BOT_TOKEN: str = os.getenv("SLACK_BOT_TOKEN")

    class Config:
        env_file = ".env"

# Example usage in redis-cli:
# PUBLISH channel_migrate_issue_tickets '{ "source_repo": "nss-group-projects/cider-falls", "all_target_repositories": ["stevebrownlee/rare-test"], "notification_channel": "C06GHMZB3M3"}'