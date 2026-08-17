Opinionated Go shared code I wrote to reuse when necessary

## Packages

- **msgque** — Message queue abstraction (pub/sub, RPC, intervals) over RabbitMQ/in-memory
- **opentel** — OpenTelemetry wrapper (traces, metrics, structured logging via spans)
- **zlog** — Thin zerolog wrapper with init gate and ordered JSON output
- **util** — Small helpers (env vars, etc.)

## Usage

```go
opentel.Init(ctx) // before msgque
msgque.Init(ctx, myKVStorage) // pass nil if not need intervals
```

## Notes

- Interval features (`CancelInterval`, `StatusInterval`, etc.) require a `valky` implementation
