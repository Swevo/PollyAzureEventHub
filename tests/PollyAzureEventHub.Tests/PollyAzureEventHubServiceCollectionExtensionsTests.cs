public class PollyAzureEventHubServiceCollectionExtensionsTests
{
    private const string FakeConnectionString =
        "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    private static readonly EventHubProducerClient _client =
        new(FakeConnectionString, "fake-hub");

    [Fact]
    public void AddPollyAzureEventHub_RegistersResiliencePipeline()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_client);
        services.AddPollyAzureEventHub(p => p.AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            Delay = TimeSpan.Zero,
            ShouldHandle = EventHubsTransientErrors.IsTransient,
        }));

        var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<ResiliencePipeline>());
    }

    [Fact]
    public void AddPollyAzureEventHub_RegistersResilientProducerClient()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_client);
        services.AddPollyAzureEventHub(p => { });

        var provider = services.BuildServiceProvider();
        var resilient = provider.GetRequiredService<ResilientEventHubProducerClient>();

        Assert.NotNull(resilient);
        Assert.Same(_client, resilient.Inner);
    }

    [Fact]
    public void AddPollyAzureEventHub_WithConnectionString_RegistersClient()
    {
        var services = new ServiceCollection();
        services.AddPollyAzureEventHub(FakeConnectionString, "fake-hub", p => { });

        var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<ResilientEventHubProducerClient>());
    }

    [Fact]
    public void AddPollyAzureEventHub_ReturnsServiceCollection()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_client);

        var result = services.AddPollyAzureEventHub(p => { });

        Assert.Same(services, result);
    }
}
