/// <summary>
/// Wraps an <see cref="EventHubProducerClient"/> with a Polly v8 <see cref="ResiliencePipeline"/>,
/// applying retry, timeout, and circuit-breaker to every send operation.
/// </summary>
public sealed class ResilientEventHubProducerClient(
    EventHubProducerClient client,
    ResiliencePipeline pipeline)
{
    /// <summary>The underlying <see cref="EventHubProducerClient"/>.</summary>
    public EventHubProducerClient Inner => client;

    /// <summary>Creates an <see cref="EventDataBatch"/>, protected by the resilience pipeline.</summary>
    public Task<EventDataBatch> CreateBatchAsync(
        CancellationToken cancellationToken = default)
        => pipeline.ExecuteAsync(
            async ct => await client.CreateBatchAsync(ct),
            cancellationToken).AsTask();

    /// <summary>Creates an <see cref="EventDataBatch"/> with options, protected by the resilience pipeline.</summary>
    public Task<EventDataBatch> CreateBatchAsync(
        CreateBatchOptions options,
        CancellationToken cancellationToken = default)
        => pipeline.ExecuteAsync(
            async ct => await client.CreateBatchAsync(options, ct),
            cancellationToken).AsTask();

    /// <summary>Sends a batch of events, protected by the resilience pipeline.</summary>
    public Task SendAsync(
        EventDataBatch eventBatch,
        CancellationToken cancellationToken = default)
        => pipeline.ExecuteAsync(
            async ct => { await client.SendAsync(eventBatch, ct); return 0; },
            cancellationToken).AsTask();

    /// <summary>Sends a collection of events, protected by the resilience pipeline.</summary>
    public Task SendAsync(
        IEnumerable<EventData> events,
        CancellationToken cancellationToken = default)
        => pipeline.ExecuteAsync(
            async ct => { await client.SendAsync(events, ct); return 0; },
            cancellationToken).AsTask();

    /// <summary>Sends a collection of events with options, protected by the resilience pipeline.</summary>
    public Task SendAsync(
        IEnumerable<EventData> events,
        SendEventOptions options,
        CancellationToken cancellationToken = default)
        => pipeline.ExecuteAsync(
            async ct => { await client.SendAsync(events, options, ct); return 0; },
            cancellationToken).AsTask();

    /// <summary>
    /// Executes any <see cref="EventHubProducerClient"/> operation, protected by the resilience pipeline.
    /// </summary>
    public Task<T> ExecuteAsync<T>(
        Func<EventHubProducerClient, CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken = default)
        => pipeline.ExecuteAsync(
            async ct => await operation(client, ct),
            cancellationToken).AsTask();
}
