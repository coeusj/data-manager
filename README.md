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

### Compile Protobuffs
```bash
protoc --proto_path=api/proto --go_out=gen/go --go_opt=paths=source_relative api/proto/event/v1/event.proto
```

## Debug

#### Kafka event example
Key:
```json
{
    "id": "${random.uuid}",
    "timestamp": "${current.datetime:yyyy'-'MM'-'dd'T'HH:mm:ss'+00:00'}"
}
```
Value:
```json
{
    "id": "${random.uuid}",
    "resourceId": "${random.string:10:20}",
    "type": "event_type",
    "start": "${current.datetime:yyyy'-'MM'-'dd'T'HH:mm:ss'+00:00'}",
    "end": "${current.datetime:yyyy'-'MM'-'dd'T'HH:mm:ss'+00:00'}"
}
```

#### Redis CLI
Run this to check/debug  Redis instance
```bash
docker compose exec redis redis-cli
```