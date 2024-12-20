# Monarch Service

## Overview

The Monarch service is a microservice that is part of the Learning Platform that handles migrating tickets from the source group project repositories to each of the student teams' repositories. The Learning Platform API sends a message to the **channel_migrate_issue_tickets** channel after repos are created, students are added as collaborators, and Slack messages have been sent.

```py
message = json.dumps({
    'notification_channel': cohort.slack_channel,
    'source_repo': project.client_template_url,
    'all_target_repositories': issue_target_repos
})

redis_client.publish('channel_migrate_issue_tickets', message)
```

The Monarch service listens for messages on the channel and kicks off issue ticket migration if the source repository has any, otherwise the migration process is skipped.

## Dependencies
- Python 3.8+
- Redis
- Django
- requests
- structlog
- pydantic
- prometheus_client
- tenacity
- pipenv

## Installation
1. Clone the repository:
    ```sh
    git clone https://github.com/stevebrownlee/learning-platform.git
    cd learning-platform/services/monarch
    ```

2. Install `pipenv` if you haven't already:
    ```sh
    pip install pipenv
    ```

3. Install the required Python packages using `pipenv`:
    ```sh
    pipenv install
    ```

4. Start the shell for the project using `pipenv`:
    ```sh
    pipenv shell
    ```
5. Open the project with your code editor.
6. Ensure Redis is installed.
7. Run the service in the terminal
    ```sh
    cd monarch
    python main.py
    ```
    or start the project in debug mode in your code editor.

## Testing with Redis-CLI

To test the Monarch service using `redis-cli`, follow these steps:

1. Start the Redis server:
    ```sh
    redis-server
    ```

2. Open a new terminal and connect to Redis CLI:
    ```sh
    redis-cli
    ```

3. Publish a test message:
    ```sh
    PUBLISH channel_migrate_issue_tickets '{ "source_repo": "nss-group-projects/cider-falls", "all_target_repositories": ["stevebrownlee/rare-test"], "notification_channel": "C06GHMZB3M3"}'
    ```

## System Diagram

```mermaid
sequenceDiagram
    participant API as Django API
    participant Redis
    participant Monarch
    participant GitHub
    participant Slack
    participant Log

    API->>Redis: Publish migration request
    Note over API,Redis: Contains source and target repos

    Redis->>Monarch: Receive message

    Monarch->>GitHub: Query source repo for issues
    GitHub-->>Monarch: Return list of issues

    alt Has issues to migrate
        loop All target repositories
           loop All issues tickets
           Monarch->>GitHub: Migrate issue to target repo
           GitHub-->>Monarch: HTTP Response
           Monarch->>Log: Issue has been migrated
           end
        end
        Monarch->>Slack: Send success notification
    else No issues found
        Monarch->>Log: Migration skipped
    end
```

## License
This project is licensed under the GNU GENERAL PUBLIC LICENSE.

