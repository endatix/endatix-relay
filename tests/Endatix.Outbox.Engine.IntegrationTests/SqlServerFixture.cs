using Testcontainers.MsSql;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Spins up a real SQL Server container once per test collection, like <see cref="PostgresFixture"/>; each
/// test creates its own uniquely-named outbox table.
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "sqlserver";
}
