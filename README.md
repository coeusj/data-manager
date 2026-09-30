## Data Manager

Data Injestion Worker

### Run worker
```bash
go run .\cmd\worker\main.go -config=<config-file-path>
```

### Run compose for local testing
```bash
docker compose -f .\docker-compose.yml up -d
```

### Event Data Example
```json
    {
        "id": "event_id",
        "resourceId": "resource_id",
        "type": "event_type",
        "start": "2026-10-01T15:04:05+00:00",
        "end": "2026-10-01T17:04:05+00:00"
    }
```