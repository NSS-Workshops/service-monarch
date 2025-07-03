"""GitHub integration for the Monarch service"""
from typing import List, Dict
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from service.models.models import Issue
from service.resilience.circuit_breaker import CircuitBreakerOpenError
from service.integrations.github_request import GithubRequest

logger = structlog.get_logger()

class GitHubIntegration:
    """Handles GitHub API interactions"""
    def __init__(self, settings, metrics, github_circuit):
        self.settings = settings
        self.metrics = metrics
        self.github_circuit = github_circuit
        self.github = GithubRequest()
        self.current_source = None

    def _github_api_call(self, func, *args, **kwargs):
        """
        Wrap GitHub API calls with circuit breaker protection.

        Args:
            func: GitHub API function to call
            *args: Arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            The result of the function

        Raises:
            Exception: Any exception raised by the function
        """
        try:
            return self.github_circuit.execute(func, *args, **kwargs)
        except CircuitBreakerOpenError as e:
            logger.error("GitHub API circuit breaker open", error=str(e))
            # Update circuit breaker state metric
            self.metrics.circuit_breaker_state.labels(circuit_name="github_api").set(1)  # 1 = open
            raise Exception(f"GitHub API unavailable: {str(e)}")
        except Exception as e:
            # Update circuit breaker failures metric
            self.metrics.circuit_breaker_failures.labels(circuit_name="github_api").inc()
            raise e

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def migrate_single_issue(
        self,
        target_repo: str,
        issue: Issue
    ) -> None:
        """ Migrate a single issue to the target repository. """
        try:
            url = f'{self.settings.GITHUB_API_URL}/repos/{target_repo}/issues'

            # Use circuit breaker for GitHub API call
            response = self._github_api_call(
                self.github.post,
                url,
                issue.model_dump()
            )

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
        """ Get all open issues from the source repository. """
        issues = []
        page = 1

        while True:
            url = (f'{self.settings.GITHUB_API_URL}/repos/{source_repo}/issues'
                  f'?state=open&direction=asc&page={page}')

            response = self._github_api_call(self.github.get, url)
            new_issues = response.json()
            logger.info(
                "github.response",
                issues=new_issues,
                status=response.status_code
            )

            if not new_issues or response.status_code != 200:
                break

            issues.extend(new_issues)
            page += 1

            # Update rate limit metric
            remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.metrics.github_rate_limit.set(remaining)

        return issues
