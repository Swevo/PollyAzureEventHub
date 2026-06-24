public class PollyAzureEventHubExtensionsTests
{
    private const string FakeConnectionString =
        "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    private static readonly EventHubProducerClient _client =
        new(FakeConnectionString, "fake-hub");

    private static readonly ResiliencePipeline _pipeline =
        new ResiliencePipelineBuilder().Build();

    [Fact]
    public void WithPolly_Pipeline_ReturnsResilientProducerClient()
    {
        var resilient = _client.WithPolly(_pipeline);

        Assert.NotNull(resilient);
        Assert.Same(_client, resilient.Inner);
    }

    [Fact]
    public void WithPolly_Configure_ReturnsResilientProducerClient()
    {
        var resilient = _client.WithPolly(p => p.AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            Delay = TimeSpan.Zero,
            ShouldHandle = EventHubsTransientErrors.IsTransient,
        }));

        Assert.NotNull(resilient);
        Assert.Same(_client, resilient.Inner);
    }

    [Fact]
    public void WithPolly_InnerIsOriginalClient()
    {
        var resilient = _client.WithPolly(_pipeline);

        Assert.Same(_client, resilient.Inner);
    }
}
