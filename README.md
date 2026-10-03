## Data Manager
Data Injestion Worker

### Compose
This compose sets up:
- Kafka
- Creates Kafka topics
- Redis
- Data Manager Worker (Go)
```bash
docker compose -f .\docker-compose.yml up -d
```

#### Worker's env variables
- DM_CONFIG_PATH

### Event Data Example
Key:
```json
    {
        "id": "event_id",
        "timestamp": "2026-10-01T15:04:05+00:00"
    }
```
Value:
```json
    {
        "id": "event_id",
        "resourceId": "resource_id",
        "type": "event_type",
        "start": "2026-10-01T15:04:05+00:00",
        "end": "2026-10-01T17:04:05+00:00"
    }
```

### Run Redis CLI
Run this is you want to check Redis instance
```bash
docker compose exec redis redis-cli
```