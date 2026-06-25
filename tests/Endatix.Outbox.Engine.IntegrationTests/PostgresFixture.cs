using Testcontainers.PostgreSql;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Spins up a real PostgreSQL container once per test collection. Shared so the (slow) container start is
/// paid once; each test still creates its own uniquely-named outbox table for isolation.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "postgres";
}
