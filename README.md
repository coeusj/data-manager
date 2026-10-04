## Data Manager
Data Injestion Worker

### Docker Compose
This compose sets up:
- Kafka
- Creates Kafka topics
- Redis
- Data Manager Worker (Go)
```bash
docker compose -f .\docker-compose.yml up -d
```

### Worker's env variables
- DM_CONFIG_PATH

### Configuration file
```json
{
    "kafka": {
        "brokers": "<kafka-1:port-1>, <kafka-2:port-2>",
        "topic": "<events-topic>",
        "groupId": "<consumer-group-id>"
    },
    "redis": {
        "address": "<redis-url:port>",
        "dataTTLSeconds": "(int)<redis-data-TTL>",
        "namespace": "<data-namespace>"
    }
}
```

## Debug

#### Kafka event example
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

#### Redis CLI
Run this to check/debug  Redis instance
```bash
docker compose exec redis redis-cli
```