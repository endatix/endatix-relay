using System.Data.Common;

namespace Endatix.Outbox.Engine;

/// <summary>
/// Supplies a fresh <see cref="DbConnection"/> for the claim store. The host implements this (or registers a
/// delegate) so the engine never references a concrete ADO.NET provider — it works against the provider's
/// connection through <see cref="System.Data.Common"/> only. Returned connections are <b>unopened</b>; the
/// claim store opens and disposes them per call (provider connection pooling makes this cheap).
/// </summary>
public interface IOutboxConnectionFactory
{
    /// <summary>Creates a new, unopened connection to the outbox database.</summary>
    DbConnection Create();
}

/// <summary>Adapts a <see cref="Func{DbConnection}"/> to <see cref="IOutboxConnectionFactory"/>.</summary>
public sealed class DelegateOutboxConnectionFactory(Func<DbConnection> factory) : IOutboxConnectionFactory
{
    private readonly Func<DbConnection> _factory = factory;

    /// <inheritdoc />
    public DbConnection Create() => _factory();
}
