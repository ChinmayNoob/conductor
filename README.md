# Conductor - A Distributed Task Scheduler 

Features

- Distributed Architecture 
- Task Priorities 
- Automatic Retries 
- Configurable Timeouts
- Output Capture
- Delayed Scheduling 
- Health Monitoring 
-Atomic Task Claiming 


Need to implement/ issues  

- task cancellation
- single point of failure issue
- no recurring tasks
- no task dependancies 

## How to run the Program 

```bash
git clone https://github.com/ChinmayNoob/conductor.git
cd conductor
docker-compose up --build
```

Open a **new terminal** and run:

```bash
go build -o client ./cmd/client
```

## Architecture

```
┌─────────────┐     HTTP      ┌─────────────┐
│   Client    │──────────────▶│  Scheduler  │
└─────────────┘               │  (port 8081)│
                              └──────┬──────┘
                                     │
                                     ▼
┌─────────────┐               ┌─────────────┐
│  PostgreSQL │◀─────────────▶│ Coordinator │
│  (port 5432)│               │ (port 8080) │
└─────────────┘               └──────┬──────┘
                                     │ gRPC
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
              ┌──────────┐    ┌──────────┐    ┌──────────┐
              │ Worker 1 │    │ Worker 2 │    │ Worker N │
              └──────────┘    └──────────┘    └──────────┘
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `postgres` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `taskscheduler` | Database name |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `postgres` | Database password |
