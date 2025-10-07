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

## Installation

You will be running this microservice as a Docker container. This service will not start successfully until you have the Valkey message broker container running.


1. Clone this repository and then:
    ```sh
    cd service-monarch
    ```
2. Open the project with your code editor
3. Copy the `.env.template` file as `.env`
4. Open the `.env` file
5. Copy the personal access token you created when you set up the Learning Platform API and paste it as the `GH_PAT` environment variable
6. Your workshop instructor will provide you with the value of the `SLACK_WEBHOOK_URL` and `SLACK_TOKEN` variables. If they haven't been provided yet, ask the instructor to share them.
6. Start the container
    ```sh
    docker compose up
    ```

## Sequence/System Diagram

```mermaid
sequenceDiagram
    title Learning Platform Ticket Migration

    API->>Valkey: PUBLISH {payload}
    Valkey->>+Monarch: RECEIVE {payload}
    activate Monarch
        Monarch->>+Github: GET /issues
        Github-->>-Monarch: List<Issue>
        opt If issues exist
            critical [Create issues on target repo]
            loop List of issues
                Monarch->>+Github: POST issue
                Github->>-Monarch: JSON
            end
            Monarch->>Slack: Migration success
            option [Exception]
                Monarch->>Slack: Migration failed
            end
        end
    deactivate Monarch
```

## Monitoring

### Viewing Logs

Access the log viewer web interface at: http://localhost:8081/

This provides a user-friendly interface to browse, filter, and search logs.

### Health Check

Monitor the service health status:
- Access the health endpoint at: http://localhost:8081/health
- Returns a JSON response with status information including:
  - Overall service status (healthy/degraded/unhealthy)
  - Valkey connection status
  - Last message processing time
  - Service uptime

### Metrics

View Prometheus metrics for monitoring service performance:
- Access metrics at: http://localhost:8080/
- Available metrics include:
  - Issue migration counts and errors
  - GitHub API rate limit status
  - Message processing times and errors
  - Connection status
  - Circuit breaker states
  - Watchdog statistics


## Architecture

For a detailed explanation of the architectural patterns and strategies used in the Monarch service, refer to the [ARCHITECTURE.md](./ARCHITECTURE.md) document.

## License
This project is licensed under the GNU GENERAL PUBLIC LICENSE.
