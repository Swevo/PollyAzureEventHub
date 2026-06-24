/// <summary>
/// Pre-built Polly <see cref="PredicateBuilder"/> for transient Azure Event Hubs errors.
/// Uses <see cref="EventHubsException.IsTransient"/> — the official Azure SDK flag —
/// so retries are always safe and aligned with the service team's guidance.
/// Also covers <see cref="TimeoutException"/> and <see cref="TaskCanceledException"/>
/// for network-level transient failures.
/// </summary>
public static class EventHubsTransientErrors
{
    /// <summary>
    /// A <see cref="PredicateBuilder"/> that handles:
    /// <list type="bullet">
    ///   <item><see cref="EventHubsException"/> where <see cref="EventHubsException.IsTransient"/> is <c>true</c></item>
    ///   <item><see cref="TimeoutException"/> — operation timed out before the service responded</item>
    ///   <item><see cref="TaskCanceledException"/> — request cancelled due to timeout or network failure</item>
    /// </list>
    /// Assign to <c>ShouldHandle</c> on any Polly strategy.
    /// </summary>
    public static readonly PredicateBuilder IsTransient =
        (PredicateBuilder)new PredicateBuilder()
            .Handle<EventHubsException>(ex => ex.IsTransient)
            .Handle<TimeoutException>()
            .Handle<TaskCanceledException>();
}
