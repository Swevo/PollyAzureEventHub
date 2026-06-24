public class EventHubsTransientErrorsTests
{
    [Fact]
    public void IsTransient_IsNotNull()
    {
        Assert.NotNull(EventHubsTransientErrors.IsTransient);
    }

    [Fact]
    public async Task IsTransient_HandlesTransientEventHubsException()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 1,
                Delay = TimeSpan.Zero,
                ShouldHandle = EventHubsTransientErrors.IsTransient,
            })
            .Build();

        var attempts = 0;
        await Assert.ThrowsAsync<EventHubsException>(() =>
            pipeline.ExecuteAsync(ct =>
            {
                attempts++;
                throw new EventHubsException(true, "test-hub");
            }).AsTask());

        Assert.Equal(2, attempts); // original + 1 retry
    }

    [Fact]
    public async Task IsTransient_DoesNotRetryNonTransientEventHubsException()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.Zero,
                ShouldHandle = EventHubsTransientErrors.IsTransient,
            })
            .Build();

        var attempts = 0;
        await Assert.ThrowsAsync<EventHubsException>(() =>
            pipeline.ExecuteAsync(ct =>
            {
                attempts++;
                throw new EventHubsException(false, "test-hub");
            }).AsTask());

        Assert.Equal(1, attempts); // no retry
    }

    [Fact]
    public async Task IsTransient_HandlesTimeoutException()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 1,
                Delay = TimeSpan.Zero,
                ShouldHandle = EventHubsTransientErrors.IsTransient,
            })
            .Build();

        var attempts = 0;
        await Assert.ThrowsAsync<TimeoutException>(() =>
            pipeline.ExecuteAsync(ct =>
            {
                attempts++;
                throw new TimeoutException("timed out");
            }).AsTask());

        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task IsTransient_HandlesTaskCanceledException()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 1,
                Delay = TimeSpan.Zero,
                ShouldHandle = EventHubsTransientErrors.IsTransient,
            })
            .Build();

        var attempts = 0;
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            pipeline.ExecuteAsync(ct =>
            {
                attempts++;
                throw new TaskCanceledException("cancelled");
            }).AsTask());

        Assert.Equal(2, attempts);
    }
}
