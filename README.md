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

## Related Packages

| Package | Downloads | Description |
|---|---|---|
| [PollyHealthChecks](https://www.nuget.org/packages/PollyHealthChecks) | [![Downloads](https://img.shields.io/nuget/dt/PollyHealthChecks.svg)](https://www.nuget.org/packages/PollyHealthChecks) | ASP.NET Core health checks for Polly v8 circuit breakers — expose circuit-breaker state (Closed, HalfOpen, Open, Isolated) as /health endpoint responses |
| [PollyBackoff](https://www.nuget.org/packages/PollyBackoff) | [![Downloads](https://img.shields.io/nuget/dt/PollyBackoff.svg)](https://www.nuget.org/packages/PollyBackoff) | Backoff delay strategies for Polly v8 resilience pipelines |
| [PollyGrpc](https://www.nuget.org/packages/PollyGrpc) | [![Downloads](https://img.shields.io/nuget/dt/PollyGrpc.svg)](https://www.nuget.org/packages/PollyGrpc) | Polly v8 resilience interceptor for gRPC |
| [PollyEFCore](https://www.nuget.org/packages/PollyEFCore) | [![Downloads](https://img.shields.io/nuget/dt/PollyEFCore.svg)](https://www.nuget.org/packages/PollyEFCore) | Polly v8 resilience pipelines for Entity Framework Core — wrap every EF Core query and SaveChanges with retry, timeout and circuit-breaker via a single AddPollyResilience() call |
| [PollyRabbitMQ](https://www.nuget.org/packages/PollyRabbitMQ) | [![Downloads](https://img.shields.io/nuget/dt/PollyRabbitMQ.svg)](https://www.nuget.org/packages/PollyRabbitMQ) | Polly v8 resilience for RabbitMQ.Client v7+ — retry, circuit-breaker, and timeout for IChannel operations, with built-in RabbitMqTransientErrors predicate covering AlreadyClosedException, BrokerUnreachableException, OperationInterruptedException, and ConnectFailureException |
| [PollyMailKit](https://www.nuget.org/packages/PollyMailKit) | [![Downloads](https://img.shields.io/nuget/dt/PollyMailKit.svg)](https://www.nuget.org/packages/PollyMailKit) | Polly v8 resilience pipelines for MailKit — retry, timeout, and circuit-breaker for SmtpClient.SendAsync and any MailKit SMTP operation |
| [PollyMassTransit](https://www.nuget.org/packages/PollyMassTransit) | [![Downloads](https://img.shields.io/nuget/dt/PollyMassTransit.svg)](https://www.nuget.org/packages/PollyMassTransit) | Polly v8 resilience pipelines for MassTransit — retry, timeout, and circuit-breaker for IBus.Publish and ISendEndpointProvider.Send |
| [PollyNpgsql](https://www.nuget.org/packages/PollyNpgsql) | [![Downloads](https://img.shields.io/nuget/dt/PollyNpgsql.svg)](https://www.nuget.org/packages/PollyNpgsql) | Polly v8 resilience pipelines for Npgsql (PostgreSQL) — retry, timeout, and circuit-breaker for NpgsqlConnection queries and commands, plus a built-in PostgresTransientErrors predicate covering all common PostgreSQL transient SQLSTATE codes |
| [PollyOpenAI](https://www.nuget.org/packages/PollyOpenAI) | [![Downloads](https://img.shields.io/nuget/dt/PollyOpenAI.svg)](https://www.nuget.org/packages/PollyOpenAI) | Polly v8 resilience for OpenAI and Azure OpenAI API calls |
| [PollySignalR](https://www.nuget.org/packages/PollySignalR) | [![Downloads](https://img.shields.io/nuget/dt/PollySignalR.svg)](https://www.nuget.org/packages/PollySignalR) | Polly v8 reconnect policy for SignalR |
| [PollyElasticsearch](https://www.nuget.org/packages/PollyElasticsearch) | [![Downloads](https://img.shields.io/nuget/dt/PollyElasticsearch.svg)](https://www.nuget.org/packages/PollyElasticsearch) | Polly v8 resilience pipelines for Elastic.Clients.Elasticsearch 8+ — retry, timeout, and circuit-breaker for any Elasticsearch operation, plus a built-in ElasticTransientErrors predicate covering rate limiting (429), service unavailability (503), gateway timeouts (504), and connection failures |
| [PollyHangfire](https://www.nuget.org/packages/PollyHangfire) | [![Downloads](https://img.shields.io/nuget/dt/PollyHangfire.svg)](https://www.nuget.org/packages/PollyHangfire) | Polly v8 resilience pipelines for Hangfire — retry, timeout, and circuit-breaker for IBackgroundJobClient.Enqueue and Schedule |
| [PollyCosmosDb](https://www.nuget.org/packages/PollyCosmosDb) | [![Downloads](https://img.shields.io/nuget/dt/PollyCosmosDb.svg)](https://www.nuget.org/packages/PollyCosmosDb) | Polly v8 resilience pipelines for Azure Cosmos DB — retry, timeout, and circuit-breaker for Container operations, plus a built-in CosmosTransientErrors predicate covering rate limiting (429), timeouts (408), partition failovers (410), and service unavailability (503) |
| [PollySendGrid](https://www.nuget.org/packages/PollySendGrid) | [![Downloads](https://img.shields.io/nuget/dt/PollySendGrid.svg)](https://www.nuget.org/packages/PollySendGrid) | Polly v8 resilience pipelines for SendGrid — retry, timeout, and circuit-breaker for ISendGridClient.SendEmailAsync |
| [PollyMongo](https://www.nuget.org/packages/PollyMongo) | [![Downloads](https://img.shields.io/nuget/dt/PollyMongo.svg)](https://www.nuget.org/packages/PollyMongo) | Polly v8 resilience pipelines for MongoDB.Driver — wrap Find, InsertOne, UpdateOne, DeleteOne and other IMongoCollection calls with retry, timeout, circuit-breaker, and more using a single ResilientMongoCollection decorator |
| [PollyDapper](https://www.nuget.org/packages/PollyDapper) | [![Downloads](https://img.shields.io/nuget/dt/PollyDapper.svg)](https://www.nuget.org/packages/PollyDapper) | Polly v8 resilience pipelines for Dapper — wrap QueryAsync, ExecuteAsync, and other Dapper calls with retry, timeout, circuit-breaker, and more using a single ResilientDbConnection decorator |
| [PollyMediatR](https://www.nuget.org/packages/PollyMediatR) | [![Downloads](https://img.shields.io/nuget/dt/PollyMediatR.svg)](https://www.nuget.org/packages/PollyMediatR) | Polly v8 resilience pipelines for MediatR — add retry, timeout, circuit-breaker, rate-limiting, hedging, and chaos engineering to any MediatR request handler with a single line of DI registration |
| [PollySqlClient](https://www.nuget.org/packages/PollySqlClient) | [![Downloads](https://img.shields.io/nuget/dt/PollySqlClient.svg)](https://www.nuget.org/packages/PollySqlClient) | Polly v8 resilience pipelines for Microsoft.Data.SqlClient (SQL Server and Azure SQL) — retry, timeout, and circuit-breaker for SqlConnection queries and commands, plus a built-in SqlServerTransientErrors predicate covering all common SQL Server and Azure SQL transient error numbers |
| [PollyAzureKeyVault](https://www.nuget.org/packages/PollyAzureKeyVault) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureKeyVault.svg)](https://www.nuget.org/packages/PollyAzureKeyVault) | Polly v8 resilience pipelines for Azure Key Vault — retry, timeout, and circuit-breaker for SecretClient, KeyClient, and CertificateClient |
| [PollyAzureQueueStorage](https://www.nuget.org/packages/PollyAzureQueueStorage) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureQueueStorage.svg)](https://www.nuget.org/packages/PollyAzureQueueStorage) | Polly v8 resilience pipelines for Azure Queue Storage — retry, timeout, and circuit-breaker for Azure.Storage.Queues QueueClient |
| [PollyRedis](https://www.nuget.org/packages/PollyRedis) | [![Downloads](https://img.shields.io/nuget/dt/PollyRedis.svg)](https://www.nuget.org/packages/PollyRedis) | Polly v8 resilience for StackExchange.Redis |
| [PollyAzureServiceBus](https://www.nuget.org/packages/PollyAzureServiceBus) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureServiceBus.svg)](https://www.nuget.org/packages/PollyAzureServiceBus) | Polly v8 resilience for Azure Service Bus — retry, circuit breaker, and timeout for sending and receiving messages |
| [PollyAzureBlob](https://www.nuget.org/packages/PollyAzureBlob) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureBlob.svg)](https://www.nuget.org/packages/PollyAzureBlob) | Polly v8 resilience pipelines for Azure Blob Storage — wrap BlobClient and BlobContainerClient operations with retry, timeout, circuit-breaker, and more using ResilientBlobClient and ResilientBlobContainerClient decorators |
| [PollyKafka](https://www.nuget.org/packages/PollyKafka) | [![Downloads](https://img.shields.io/nuget/dt/PollyKafka.svg)](https://www.nuget.org/packages/PollyKafka) | Polly v8 resilience for Confluent.Kafka — retry, circuit breaker, and timeout for producers and consumers |
| [PollyAzureTableStorage](https://www.nuget.org/packages/PollyAzureTableStorage) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureTableStorage.svg)](https://www.nuget.org/packages/PollyAzureTableStorage) | Polly v8 resilience pipelines for Azure Table Storage — retry, timeout, and circuit-breaker for Azure.Data.Tables TableClient |

## 💼 Need .NET consulting?

The author of this package is available for consulting on **Polly v8 resilience**, **Azure cloud architecture**, and **clean .NET design**.

**[→ solidqualitysolutions.com](https://www.solidqualitysolutions.com/)** · **[LinkedIn](https://www.linkedin.com/in/justbannister/)**
## License

MIT © [Justin Bannister](https://github.com/Swevo)