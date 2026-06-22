namespace Endatix.Outbox.Engine;

/// <summary>Feature-flag keys owned by the outbox engine.</summary>
public static class OutboxFlags
{
    /// <summary>
    /// Gates the in-process relay. <c>true</c> = this process claims and publishes; <c>false</c> = it
    /// stands down (rows accumulate as pending and drain when re-enabled). Flipping to <c>false</c> is the
    /// cutover to a standalone relay worker. Defaults to <c>true</c> when no provider resolves it.
    /// </summary>
    public const string RelayInProcess = "outbox-relay-in-process";
}
