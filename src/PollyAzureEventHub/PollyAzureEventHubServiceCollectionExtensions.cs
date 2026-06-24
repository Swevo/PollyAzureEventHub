/// <summary>Dependency-injection extensions for <c>PollyAzureEventHub</c>.</summary>
public static class PollyAzureEventHubServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="ResiliencePipeline"/> built by <paramref name="configure"/>
    /// and a transient <see cref="ResilientEventHubProducerClient"/> that wraps the
    /// <see cref="EventHubProducerClient"/> already registered in the DI container.
    /// </summary>
    public static IServiceCollection AddPollyAzureEventHub(
        this IServiceCollection services,
        Action<ResiliencePipelineBuilder> configure)
    {
        var builder = new ResiliencePipelineBuilder();
        configure(builder);
        var pipeline = builder.Build();

        services.AddSingleton(pipeline);
        services.AddTransient<ResilientEventHubProducerClient>(sp =>
            sp.GetRequiredService<EventHubProducerClient>().WithPolly(pipeline));

        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="EventHubProducerClient"/> for the given
    /// <paramref name="connectionString"/> and <paramref name="eventHubName"/>,
    /// then registers the resilience pipeline and <see cref="ResilientEventHubProducerClient"/>.
    /// </summary>
    public static IServiceCollection AddPollyAzureEventHub(
        this IServiceCollection services,
        string connectionString,
        string eventHubName,
        Action<ResiliencePipelineBuilder> configure)
    {
        services.AddSingleton(new EventHubProducerClient(connectionString, eventHubName));
        return services.AddPollyAzureEventHub(configure);
    }
}
