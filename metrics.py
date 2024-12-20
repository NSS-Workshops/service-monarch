from prometheus_client import Counter, Gauge

class Metrics:
    def __init__(self):
        self.issues_migrated = Counter(
            'issues_migrated_total',
            'Number of issues migrated',
            ['source_repo', 'target_repo']
        )
        self.migration_errors = Counter(
            'migration_errors_total',
            'Number of migration errors',
            ['source_repo', 'target_repo']
        )
        self.github_rate_limit = Gauge(
            'github_rate_limit_remaining',
            'Remaining GitHub API rate limit'
        )
