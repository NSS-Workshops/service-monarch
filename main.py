import os
from typing import List, Dict
from time import sleep
import redis
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential
from prometheus_client import start_http_server

from config import Settings
from models import IssueTemplate, Issue, MigrationData
from github_request import GithubRequest
from slack import SlackAPI
from metrics import Metrics

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

class TicketMigrator:
    def __init__(self):
        self.current_source = None
        self.settings = Settings()
        self.github = GithubRequest()
        self.redis_client = redis.StrictRedis(
            host=self.settings.REDIS_HOST,
            port=self.settings.REDIS_PORT,
            db=self.settings.REDIS_DB
        )
        self.metrics = Metrics()

    def format_issue(self, template_data: IssueTemplate) -> str:
        template_file = os.path.join(self.settings.TEMPLATE_DIR, 'issue.md')
        with open(template_file, 'r') as f:
            template = f.read()

        return template.format(**template_data.dict())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def migrate_single_issue(
        self,
        target_repo: str,
        issue: Issue
    ) -> None:
        try:
            url = f'{self.settings.GITHUB_API_URL}/repos/{target_repo}/issues'
            response = self.github.post(url, issue.model_dump())

            if response.status_code != 201:
                raise Exception(f'Failed to create issue. {response.text}')

            self.metrics.issues_migrated.labels(
                source_repo=self.current_source,
                target_repo=target_repo
            ).inc()

            # Update rate limit metric
            remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.metrics.github_rate_limit.set(remaining)

            logger.info(
                "issue.migrated",
                target_repo=target_repo,
                issue_title=issue.title
            )

        except Exception as e:
            self.metrics.migration_errors.labels(
                source_repo=self.current_source,
                target_repo=target_repo
            ).inc()

            logger.error(
                "issue.migration_failed",
                target_repo=target_repo,
                issue_title=issue.title,
                error=str(e)
            )
            raise

    async def get_source_issues(self, source_repo: str) -> List[Dict]:
        issues = []
        page = 1

        while True:
            url = (f'{self.settings.GITHUB_API_URL}/repos/{source_repo}/issues'
                  f'?state=open&direction=asc&page={page}')

            response = self.github.get(url)
            new_issues = response.json()

            if not new_issues:
                break

            issues.extend(new_issues)
            page += 1

            # Update rate limit metric
            remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.metrics.github_rate_limit.set(remaining)

        return issues

    async def migrate_tickets(self, data: MigrationData) -> None:
        slack = SlackAPI()
        self.current_source = data.source_repo

        try:
            # Get all issues from source repo
            source_issues = await self.get_source_issues(data.source_repo)

            if not source_issues:
                logger.info(
                    "migration.skipped",
                    source_repo=data.source_repo,
                    reason="No issues found in source repository"
                )
                return


            # Format the issues for migration
            issues_to_migrate = []
            for issue in source_issues:
                template_data = IssueTemplate(
                    user_name=issue['user']['login'],
                    user_url=issue['user']['html_url'],
                    user_avatar=issue['user']['avatar_url'],
                    date=issue['created_at'],
                    url=issue['html_url'],
                    body=issue['body']
                )

                new_issue = Issue(
                    title=issue['title'],
                    body=self.format_issue(template_data)
                )
                issues_to_migrate.append(new_issue)

            # Migrate issues to each target repo
            for target in data.all_target_repositories:
                messages = []

                for issue in issues_to_migrate:
                    try:
                        await self.migrate_single_issue(target, issue)
                    except Exception as e:
                        messages.append(
                            f'Error creating issue {issue.title}. {str(e)}.'
                        )

                    # Pause between issues to prevent rate limiting
                    sleep(self.settings.GITHUB_RATE_LIMIT_PAUSE)

                # Send status message to Slack
                if messages:
                    await slack.send_message(
                        data.notification_channel,
                        '\n'.join(messages)
                    )
                else:
                    await slack.send_message(
                        data.notification_channel,
                        f'All issues migrated successfully to {target}.'
                    )

                # Pause between repositories
                # sleep(self.settings.REPO_MIGRATION_PAUSE)
                sleep(10)

        except Exception as e:
            logger.error(
                "migration.failed",
                source_repo=data.source_repo,
                error=str(e)
            )
            await slack.send_message(
                data.notification_channel,
                f'Migration failed: {str(e)}'
            )
            raise

    async def run(self):
        # Start Prometheus metrics server
        start_http_server(8000)

        pubsub = self.redis_client.pubsub()
        pubsub.subscribe('channel_migrate_issue_tickets')
        logger.info('Waiting for messages. To exit press CTRL+C')

        try:
            for message in pubsub.listen():
                if message['type'] == 'message':
                    data = MigrationData.model_validate_json(message['data'])
                    await self.migrate_tickets(data)
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error("Fatal error", error=str(e))
            raise

if __name__ == "__main__":
    import asyncio

    migrator = TicketMigrator()
    asyncio.run(migrator.run())