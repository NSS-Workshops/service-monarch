# Monarch Service

## Overview

Monarch is an independent service that is a part of the Learning Platform system that handles migrating tickets from the source group project repositories to each of the student teams' repositories. The Learning Platform API sends a message to a Valkey instance on the **channel_migrate_issue_tickets** channel after repos are created, students are added as collaborators, and Slack messages have been sent.

```py
message = json.dumps({
    'notification_channel': cohort.slack_channel,
    'source_repo': project.client_template_url,
    'all_target_repositories': issue_target_repos
})

valkey_client.publish('channel_migrate_issue_tickets', message)
```

The Monarch service listens for messages on the channel and kicks off issue ticket migration if the source repository has any, otherwise the migration process is skipped.

```py
pubsub = self.valkey_client.pubsub()
pubsub.subscribe('channel_migrate_issue_tickets')

for message in pubsub.listen():
    if message['type'] == 'message':
        data = MigrationData.model_validate_json(message['data'])
        await self.migrate_tickets(data)
```

## System Dependencies

- Python 3.10+
- [Valkey](https://valkey.io/topics/installation/)
- [valkey-py](https://github.com/valkey-io/valkey-py)
- [pipenv](https://pipenv.pypa.io/en/latest/) virtual environment manager

## Service Dependencies

- [requests](https://docs.python-requests.org/en/latest/index.html) for HTTP communication with Github and Slack
- [structlog](https://www.structlog.org/en/stable/index.html) for logging
- [pydantic](https://docs.pydantic.dev/latest/) for data validation
- [prometheus_client](https://prometheus.github.io/client_python/getting-started/three-step-demo/) for metrics
- [tenacity](https://tenacity.readthedocs.io/en/latest/) for HTTP request retrying

## Installation
1. Clone the repository:
    ```sh
    git clone git@github.com:stevebrownlee/service-monarch.git
    cd service-monarch
    ```

2. Install the required Python packages using `pipenv`:
    ```sh
    pipenv install
    ```

3. Start the shell for the project using `pipenv`:
    ```sh
    pipenv shell
    ```
4. Open the project with your code editor.
5. Ensure Valkey is installed.
6. Run the service in the terminal
    ```sh
    cd monarch
    python main.py
    ```
    or start the project in debug mode in your code editor.

## Testing with Valkey CLI

To test the Monarch service using `valkey-cli`, follow these steps:

1. Start the Valkey server on Mac:
    ```sh
    brew services start valkey
    ```

2. Open a new terminal and connect to Valkey CLI:
    ```sh
    valkey-client
    ```

3. Publish a test message:
    ```sh
    PUBLISH channel_migrate_issue_tickets '{ "source_repo": "nss-group-projects/cider-falls", "all_target_repositories": ["stevebrownlee/rare-test"], "notification_channel": "C06GHMZB3M3"}'
    ```

## Sequence/System Diagram

```mermaid
sequenceDiagram
    participant API as Django API
    participant Valkey
    participant Monarch
    participant GitHub
    participant Slack
    participant Log

    API->>Valkey: Publish migration request
    Note over API,Valkey: Source/target repos + Slack channel

    Valkey->>Monarch: Receive message

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

