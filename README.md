# PollyAzureEventHub

[![NuGet](https://img.shields.io/nuget/v/PollyAzureEventHub.svg)](https://www.nuget.org/packages/PollyAzureEventHub/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/PollyAzureEventHub.svg)](https://www.nuget.org/packages/PollyAzureEventHub/)
[![CI](https://github.com/Swevo/PollyAzureEventHub/actions/workflows/build.yml/badge.svg)](https://github.com/Swevo/PollyAzureEventHub/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Polly v8 resilience for `Azure.Messaging.EventHubs`** — add retry, timeout, and circuit-breaker to any Event Hubs producer operation in two lines.

```csharp
var producer = new EventHubProducerClient(connectionString, "my-hub");

var resilient = producer.WithPolly(pipeline => pipeline
    .AddRetry(new RetryStrategyOptions
    {
        MaxRetryAttempts = 3,
        Delay = TimeSpan.FromSeconds(2),
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        ShouldHandle = EventHubsTransientErrors.IsTransient,
    })
    .AddTimeout(TimeSpan.FromSeconds(30)));

using var batch = await resilient.CreateBatchAsync();
batch.TryAdd(new EventData("hello"));
await resilient.SendAsync(batch);
```

## Why PollyAzureEventHub?

Azure Event Hubs is a mission-critical ingestion pipeline — dropped events mean lost data. `EventHubsException.IsTransient` tells you exactly which errors are safe to retry; this library wires that directly into Polly v8:

| Problem | Solution |
|---------|----------|
| `EventHubsException` where `IsTransient = true` (throttling, service busy, connection dropped) | Caught by `EventHubsTransientErrors.IsTransient` |
| `TimeoutException` — service took too long to respond | Caught by `EventHubsTransientErrors.IsTransient` |
| `TaskCanceledException` — network timeout during transit | Caught by `EventHubsTransientErrors.IsTransient` |
| Non-retriable errors (bad request, auth failure) | Not retried — `IsTransient = false` is respected |
| Cascading failures during an outage | Wrap with `AddCircuitBreaker` |

## Installation

```
dotnet add package PollyAzureEventHub
dotnet add package Polly.Core
```

## Quick-start

### 1. Manual wiring

```csharp
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Polly;
using Polly.Retry;

var producer = new EventHubProducerClient(connectionString, "telemetry");

var resilient = producer.WithPolly(p => p
    .AddRetry(new RetryStrategyOptions
    {
        MaxRetryAttempts = 3,
        Delay = TimeSpan.FromSeconds(2),
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        ShouldHandle = EventHubsTransientErrors.IsTransient,
    }));

// Send a batch
using var batch = await resilient.CreateBatchAsync();
foreach (var reading in sensorReadings)
    batch.TryAdd(new EventData(JsonSerializer.SerializeToUtf8Bytes(reading)));

await resilient.SendAsync(batch);
```

### 2. Dependency injection

```csharp
// Program.cs / Startup.cs
builder.Services.AddPollyAzureEventHub(
    connectionString,
    "telemetry",
    pipeline => pipeline
        .AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            Delay = TimeSpan.FromSeconds(2),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
            ShouldHandle = EventHubsTransientErrors.IsTransient,
        })
        .AddTimeout(TimeSpan.FromSeconds(30)));

// Inject ResilientEventHubProducerClient into your services
public class TelemetryIngester(ResilientEventHubProducerClient producer)
{
    public async Task SendAsync(IEnumerable<Reading> readings, CancellationToken ct)
    {
        using var batch = await producer.CreateBatchAsync(ct);
        foreach (var r in readings)
            batch.TryAdd(new EventData(JsonSerializer.SerializeToUtf8Bytes(r)));
        await producer.SendAsync(batch, ct);
    }
}
```

### 3. Bring your own client (DI overload)

```csharp
builder.Services.AddSingleton(new EventHubProducerClient(
    fullyQualifiedNamespace,
    "telemetry",
    new DefaultAzureCredential()));

builder.Services.AddPollyAzureEventHub(pipeline => pipeline
    .AddRetry(new RetryStrategyOptions
    {
        MaxRetryAttempts = 5,
        ShouldHandle = EventHubsTransientErrors.IsTransient,
    }));
```

## Transient error reference

```csharp
// Use in any Polly strategy:
ShouldHandle = EventHubsTransientErrors.IsTransient
```

| Condition | Why it's transient |
|-----------|-------------------|
| `EventHubsException` (`IsTransient = true`) | Service throttling, quota exceeded, brief outage — SDK-designated as safe to retry |
| `EventHubsException` (`IsTransient = false`) | Auth failure, bad request — **not retried** |
| `TimeoutException` | Operation timed out waiting for service response |
| `TaskCanceledException` | Network-level cancellation or timeout |

> **Key differentiator:** `EventHubsException.IsTransient` is set by the Azure SDK team — this library exposes it directly as a Polly predicate, so your retry logic is always in sync with the SDK's own classification.

## Advanced pipelines

### Full production pipeline with observability

```csharp
producer.WithPolly(p => p
    .AddTimeout(TimeSpan.FromSeconds(60))
    .AddRetry(new RetryStrategyOptions
    {
        MaxRetryAttempts = 5,
        Delay = TimeSpan.FromSeconds(1),
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        ShouldHandle = EventHubsTransientErrors.IsTransient,
        OnRetry = args =>
        {
            logger.LogWarning("Event Hubs retry {Attempt} after {Delay}ms — {Exception}",
                args.AttemptNumber, args.RetryDelay.TotalMilliseconds, args.Outcome.Exception?.Message);
            return ValueTask.CompletedTask;
        },
    })
    .AddCircuitBreaker(new CircuitBreakerStrategyOptions
    {
        FailureRatio = 0.5,
        SamplingDuration = TimeSpan.FromSeconds(30),
        MinimumThroughput = 10,
        BreakDuration = TimeSpan.FromSeconds(30),
        ShouldHandle = EventHubsTransientErrors.IsTransient,
    }));
```

### Sending with partition key

```csharp
var options = new SendEventOptions { PartitionKey = deviceId };
await resilient.SendAsync(events, options, cancellationToken);
```

### Arbitrary operations via `ExecuteAsync`

```csharp
var partitionProps = await resilient.ExecuteAsync(
    (c, ct) => c.GetPartitionPropertiesAsync("0", ct));
```

## API reference

### `ResilientEventHubProducerClient`

| Member | Description |
|--------|-------------|
| `Inner` | The underlying `EventHubProducerClient` |
| `CreateBatchAsync(ct)` | Creates a batch through the pipeline |
| `CreateBatchAsync(options, ct)` | Creates a batch with options through the pipeline |
| `SendAsync(batch, ct)` | Sends a batch through the pipeline |
| `SendAsync(events, ct)` | Sends a collection of events through the pipeline |
| `SendAsync(events, options, ct)` | Sends events with partition options through the pipeline |
| `ExecuteAsync<T>(operation, ct)` | Runs any `EventHubProducerClient` operation through the pipeline |

### `EventHubsTransientErrors`

| Member | Description |
|--------|-------------|
| `IsTransient` | `PredicateBuilder` for transient `EventHubsException`, `TimeoutException`, `TaskCanceledException` |

### Extension methods

| Method | Description |
|--------|-------------|
| `client.WithPolly(pipeline)` | Wraps an `EventHubProducerClient` with a pre-built `ResiliencePipeline` |
| `client.WithPolly(configure)` | Builds a pipeline inline and wraps the client |

### DI extensions

| Method | Description |
|--------|-------------|
| `services.AddPollyAzureEventHub(configure)` | Registers `ResiliencePipeline` + `ResilientEventHubProducerClient` (requires `EventHubProducerClient` already in DI) |
| `services.AddPollyAzureEventHub(connectionString, hubName, configure)` | Registers `EventHubProducerClient`, pipeline, and resilient client |

## Target frameworks

| Framework | Supported |
|-----------|-----------|
| .NET 6 | ✅ |
| .NET 8 | ✅ |
| .NET 9 | ✅ |

## Related packages

| Package | Description |
|---------|-------------|
| [PollyAzureServiceBus](https://github.com/Swevo/PollyAzureServiceBus) | Polly v8 for Azure Service Bus |
| [PollyAzureBlob](https://github.com/Swevo/PollyAzureBlob) | Polly v8 for Azure Blob Storage |
| [PollyAzureKeyVault](https://github.com/Swevo/PollyAzureKeyVault) | Polly v8 for Azure Key Vault |
| [PollyCosmosDb](https://github.com/Swevo/PollyCosmosDb) | Polly v8 for Azure Cosmos DB |
| [PollyKafka](https://github.com/Swevo/PollyKafka) | Polly v8 for Confluent.Kafka |
| [PollyRabbitMQ](https://github.com/Swevo/PollyRabbitMQ) | Polly v8 for RabbitMQ |
| [PollySignalR](https://github.com/Swevo/PollySignalR) | Polly v8 for SignalR |
| [PollyRedis](https://github.com/Swevo/PollyRedis) | Polly v8 for StackExchange.Redis |
| [PollyElasticsearch](https://github.com/Swevo/PollyElasticsearch) | Polly v8 for Elastic.Clients.Elasticsearch |
| [PollyEFCore](https://github.com/Swevo/PollyEFCore) | Polly v8 for Entity Framework Core |
| [PollyDapper](https://github.com/Swevo/PollyDapper) | Polly v8 for Dapper |
| [PollyMongo](https://github.com/Swevo/PollyMongo) | Polly v8 for MongoDB |
| [PollyNpgsql](https://github.com/Swevo/PollyNpgsql) | Polly v8 for Npgsql (PostgreSQL) |
| [PollySqlClient](https://github.com/Swevo/PollySqlClient) | Polly v8 for Microsoft.Data.SqlClient |
| [PollyGrpc](https://github.com/Swevo/PollyGrpc) | Polly v8 for gRPC |
| [PollyOpenAI](https://github.com/Swevo/PollyOpenAI) | Polly v8 for OpenAI .NET SDK |
| [PollyMediatR](https://github.com/Swevo/PollyMediatR) | Polly v8 for MediatR |
| [PollyHealthChecks](https://github.com/Swevo/PollyHealthChecks) | Polly v8 for ASP.NET Core Health Checks |
| [PollyBackoff](https://github.com/Swevo/PollyBackoff) | Polly v8 backoff helpers |

## License

MIT © [Justin Bannister](https://github.com/Swevo)