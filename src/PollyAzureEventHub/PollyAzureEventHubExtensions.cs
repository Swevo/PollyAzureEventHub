/// <summary>Extension methods for adding Polly resilience to Azure Event Hubs clients.</summary>
public static class PollyAzureEventHubExtensions
{
    /// <summary>Wraps an <see cref="EventHubProducerClient"/> with the given <see cref="ResiliencePipeline"/>.</summary>
    public static ResilientEventHubProducerClient WithPolly(
        this EventHubProducerClient client,
        ResiliencePipeline pipeline)
        => new(client, pipeline);

    /// <summary>Wraps an <see cref="EventHubProducerClient"/> with a pipeline built by <paramref name="configure"/>.</summary>
    public static ResilientEventHubProducerClient WithPolly(
        this EventHubProducerClient client,
        Action<ResiliencePipelineBuilder> configure)
    {
        var builder = new ResiliencePipelineBuilder();
        configure(builder);
        return new(client, builder.Build());
    }
}
